"""
Provision per-tier Genie spaces, dedicated serverless warehouses, and service principals.

Max-rigor attribution:
  - LLM $  → system.billing.usage identity_metadata.run_as = tier SP
  - SQL $  → usage_metadata.warehouse_id = tier warehouse

Supports --tiers-csv for orchestrator jobs (e.g. t0,t1,t4).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Channel,
    ChannelName,
    CreateWarehouseRequestWarehouseType,
)

from genie_bench.config_utils import REPO_ROOT, load_benchmark_config
from genie_bench.spaces.compile_space import OUT_DIR, compile_all
from genie_bench.sql_exec import execute_sql

STATE_PATH = REPO_ROOT / "src" / "genie_bench" / "spaces" / "provisioned_state.json"
DEFAULT_SECRETS_SCOPE = "genie-tco-bench"


def _desired_channel(channel: str | ChannelName | None) -> Channel:
    """Resolve warehouse channel. Preview enables window measures + one-to-many joins."""
    if isinstance(channel, ChannelName):
        return Channel(name=channel)
    name = (channel or "PREVIEW").strip().upper()
    mapping = {
        "CURRENT": ChannelName.CHANNEL_NAME_CURRENT,
        "PREVIEW": ChannelName.CHANNEL_NAME_PREVIEW,
        "PREVIOUS": ChannelName.CHANNEL_NAME_PREVIOUS,
        "CUSTOM": ChannelName.CHANNEL_NAME_CUSTOM,
        "CHANNEL_NAME_CURRENT": ChannelName.CHANNEL_NAME_CURRENT,
        "CHANNEL_NAME_PREVIEW": ChannelName.CHANNEL_NAME_PREVIEW,
        "CHANNEL_NAME_PREVIOUS": ChannelName.CHANNEL_NAME_PREVIOUS,
        "CHANNEL_NAME_CUSTOM": ChannelName.CHANNEL_NAME_CUSTOM,
    }
    return Channel(name=mapping.get(name, ChannelName.CHANNEL_NAME_PREVIEW))


def ensure_warehouse(
    w: WorkspaceClient,
    name: str,
    size: str,
    auto_stop: int,
    channel: str | ChannelName | None = "PREVIEW",
) -> str:
    desired = _desired_channel(channel)
    existing = [x for x in w.warehouses.list() if x.name == name]
    if existing:
        wh_id = existing[0].id  # type: ignore[assignment]
        print(f"Warehouse exists: {name} ({wh_id})")
        # Align channel so preview MV features (window measures, one-to-many) compile.
        try:
            info = w.warehouses.get(id=wh_id)
            current = getattr(getattr(info, "channel", None), "name", None)
            desired_name = desired.name
            if current != desired_name:
                w.warehouses.edit(id=wh_id, channel=desired)
                print(f"Updated warehouse {name} channel {current} -> {desired_name}")
        except Exception as e:  # noqa: BLE001
            print(f"Warning: could not update channel on {name}: {e}")
        return wh_id  # type: ignore[return-value]

    created = w.warehouses.create(
        name=name,
        cluster_size=size,
        auto_stop_mins=auto_stop,
        enable_serverless_compute=True,
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        max_num_clusters=1,
        channel=desired,
    )
    wh_id = created.id
    print(f"Created warehouse {name} -> {wh_id} (channel={desired.name})")
    for _ in range(60):
        info = w.warehouses.get(id=wh_id)
        state = str(info.state) if info.state else ""
        if "RUNNING" in state or "STOPPED" in state or "STARTING" in state:
            break
        time.sleep(2)
    return wh_id  # type: ignore[return-value]


def ensure_service_principal(w: WorkspaceClient, display_name: str) -> dict[str, str]:
    existing = [sp for sp in w.service_principals.list() if sp.display_name == display_name]
    if existing:
        sp = existing[0]
        print(f"SP exists: {display_name} ({sp.application_id})")
        return {"id": str(sp.id), "application_id": sp.application_id, "display_name": display_name}  # type: ignore

    created = w.service_principals.create(display_name=display_name)
    print(f"Created SP {display_name} -> {created.application_id}")
    return {
        "id": str(created.id),  # type: ignore
        "application_id": created.application_id,  # type: ignore
        "display_name": display_name,
    }


def ensure_secrets_scope(w: WorkspaceClient, scope: str) -> None:
    existing = {s.name for s in w.secrets.list_scopes()}
    if scope in existing:
        return
    try:
        w.secrets.create_scope(scope=scope)
        print(f"Created secret scope {scope}")
    except Exception as e:  # noqa: BLE001
        print(f"Secret scope create warning ({scope}): {e}")


def ensure_sp_entitlements(w: WorkspaceClient, sp_id: str) -> None:
    """Grant workspace-access + databricks-sql-access so the SP can call Genie/SQL APIs."""
    body = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [
            {
                "op": "add",
                "path": "entitlements",
                "value": [
                    {"value": "workspace-access"},
                    {"value": "databricks-sql-access"},
                ],
            }
        ],
    }
    try:
        w.api_client.do("PATCH", f"/api/2.0/preview/scim/v2/ServicePrincipals/{sp_id}", body=body)
        print(f"Ensured entitlements on SP {sp_id}")
    except Exception as e:  # noqa: BLE001
        print(f"SP entitlements warning ({sp_id}): {e}")


def ensure_sp_oauth_secret(
    w: WorkspaceClient,
    sp: dict[str, str],
    tier_key: str,
    scope: str,
) -> dict[str, str]:
    """Create an OAuth secret for the SP and store client id/secret in the secret scope.

    Returns metadata (never returns the raw secret after first create if already present).
    """
    client_id_key = f"sp-{tier_key}-client-id"
    client_secret_key = f"sp-{tier_key}-client-secret"
    ensure_secrets_scope(w, scope)

    # Always refresh the client-id (application id is public)
    try:
        w.secrets.put_secret(scope=scope, key=client_id_key, string_value=sp["application_id"])
    except Exception as e:  # noqa: BLE001
        print(f"Secret put warning ({client_id_key}): {e}")

    # If a secret already exists in the scope, keep it (do not rotate every provision)
    existing_keys = {s.key for s in w.secrets.list_secrets(scope=scope)}
    if client_secret_key in existing_keys:
        print(f"SP OAuth secret already in scope for {tier_key}")
        return {"secrets_scope": scope, "client_id_key": client_id_key, "client_secret_key": client_secret_key}

    try:
        created = w.service_principal_secrets_proxy.create(service_principal_id=sp["id"])
        secret_value = created.secret
        if not secret_value:
            raise RuntimeError("SP secret create returned empty secret")
        w.secrets.put_secret(scope=scope, key=client_secret_key, string_value=secret_value)
        print(f"Stored new SP OAuth secret for {tier_key} in scope {scope}")
    except Exception as e:  # noqa: BLE001
        print(f"SP OAuth secret warning ({tier_key}): {e}")
    return {"secrets_scope": scope, "client_id_key": client_id_key, "client_secret_key": client_secret_key}


def grant_uc(w: WorkspaceClient, warehouse_id: str, principal: str, catalog: str, schema: str) -> None:
    grants = [
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `{principal}`",
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{principal}`",
        f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{principal}`",
        f"GRANT EXECUTE ON SCHEMA {catalog}.{schema} TO `{principal}`",
    ]
    for sql in grants:
        try:
            execute_sql(w, warehouse_id, sql)
            print(f"OK: {sql}")
        except Exception as e:  # noqa: BLE001
            print(f"Grant warning ({sql}): {e}")


def grant_warehouse_use(w: WorkspaceClient, warehouse_id: str, sp_application_id: str) -> None:
    body = {
        "access_control_list": [
            {"service_principal_name": sp_application_id, "permission_level": "CAN_USE"}
        ]
    }
    try:
        w.api_client.do("PATCH", f"/api/2.0/permissions/warehouses/{warehouse_id}", body=body)
        print(f"Granted CAN_USE on warehouse {warehouse_id} to {sp_application_id}")
    except Exception as e:  # noqa: BLE001
        print(f"Warehouse grant warning: {e}")


def grant_genie_run(w: WorkspaceClient, space_id: str, sp_application_id: str) -> None:
    body = {
        "access_control_list": [
            {"service_principal_name": sp_application_id, "permission_level": "CAN_RUN"}
        ]
    }
    try:
        w.api_client.do("PATCH", f"/api/2.0/preview/permissions/genie/{space_id}", body=body)
        print(f"Granted CAN_RUN on genie space {space_id} to {sp_application_id}")
    except Exception as e:  # noqa: BLE001
        print(f"Genie grant warning: {e}")


def create_or_update_space(
    w: WorkspaceClient,
    display_name: str,
    warehouse_id: str,
    serialized: dict[str, Any],
    description: str,
    existing_space_id: str | None = None,
) -> str:
    """Create or update Genie space via Spaces API with serialized_space string payload."""
    body = {
        "title": display_name,
        "description": description or display_name,
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(serialized),
    }
    if existing_space_id:
        try:
            # Update existing space in-place
            w.api_client.do("PUT", f"/api/2.0/genie/spaces/{existing_space_id}", body=body)
            print(f"Updated space {display_name} -> {existing_space_id}")
            return existing_space_id
        except Exception as e:  # noqa: BLE001
            print(f"Update failed ({existing_space_id}), creating new: {e}")
    resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
    space_id = resp.get("space_id") or resp.get("id")
    print(f"Created space {display_name} -> {space_id}")
    return space_id


def _parse_tiers(tiers: list[str] | None, tiers_csv: str | None) -> list[str]:
    if tiers_csv:
        return [t.strip().lower() for t in tiers_csv.split(",") if t.strip()]
    if tiers:
        return [t.lower() for t in tiers]
    return ["t0", "t4"]


def provision(
    tiers: list[str],
    catalog: str,
    schema: str,
    warehouse_size: str = "Medium",
    auto_stop: int = 10,
    secrets_scope: str = DEFAULT_SECRETS_SCOPE,
    channel: str = "PREVIEW",
) -> dict[str, Any]:
    w = WorkspaceClient()
    compile_all(catalog, schema, tiers)

    prev: dict[str, Any] = {}
    if STATE_PATH.exists():
        try:
            prev = json.loads(STATE_PATH.read_text())
        except Exception:  # noqa: BLE001
            prev = {}

    state: dict[str, Any] = {
        "catalog": catalog,
        "schema": schema,
        "secrets_scope": secrets_scope,
        "warehouse_channel": channel,
        "tiers": dict(prev.get("tiers") or {}),
    }

    eval_wh = ensure_warehouse(w, "genie-tco-eval", warehouse_size, auto_stop, channel=channel)
    state["warehouse_id_eval"] = eval_wh
    errors: list[str] = []

    for t in tiers:
        key = t.lower()
        try:
            meta_path = OUT_DIR / f"{key}.serialized.json"
            meta = json.loads(meta_path.read_text())
            tier_code = meta["tier"]

            wh_name = f"genie-tco-{key}"
            sp_name = f"genie-tco-sp-{key}"
            wh_id = ensure_warehouse(w, wh_name, warehouse_size, auto_stop, channel=channel)
            sp = ensure_service_principal(w, sp_name)
            ensure_sp_entitlements(w, sp["id"])
            oauth = ensure_sp_oauth_secret(w, sp, key, secrets_scope)
            grant_uc(w, eval_wh, sp["application_id"], catalog, schema)
            grant_warehouse_use(w, wh_id, sp["application_id"])

            existing_space = (state.get("tiers") or {}).get(key, {}).get("space_id")
            space_id = create_or_update_space(
                w,
                meta["display_name"],
                wh_id,
                meta["serialized_space"],
                meta["description"],
                existing_space_id=existing_space,
            )
            grant_genie_run(w, space_id, sp["application_id"])

            state["tiers"][key] = {
                "tier": tier_code,
                "axis": meta.get("axis"),
                "lever": meta.get("lever"),
                "intent": meta.get("intent"),
                "space_id": space_id,
                "warehouse_id": wh_id,
                "sp_application_id": sp["application_id"],
                "sp_id": sp["id"],
                "oauth": oauth,
                "manual_steps": meta.get("manual_steps", []),
            }
            print(f"Provisioned {tier_code}: space={space_id} wh={wh_id} sp={sp['application_id']}")
            # Persist after each tier so waves can resume
            STATE_PATH.write_text(json.dumps(state, indent=2))
        except Exception as e:  # noqa: BLE001
            msg = f"{key}: {e}"
            errors.append(msg)
            print(f"ERROR provisioning {key}: {e}")
            STATE_PATH.write_text(json.dumps(state, indent=2))

    _write_bundle_overrides(state)
    print(f"Wrote state -> {STATE_PATH}")
    if errors:
        print(f"Completed with {len(errors)} errors:")
        for e in errors:
            print(f"  - {e}")
        raise SystemExit(1)
    return state


def _write_bundle_overrides(state: dict[str, Any]) -> None:
    """Write a resources snippet with provisioned IDs (dynamic; no hard-coded tier list)."""
    lines = [
        "# Auto-generated by provision_spaces.py — do not edit by hand\n",
        "# Consumed optionally; provisioned_state.json is the source of truth.\n",
        "variables:\n",
    ]
    if state.get("warehouse_id_eval"):
        lines.append(f'  warehouse_id_eval:\n    default: "{state["warehouse_id_eval"]}"\n')
    if state.get("secrets_scope"):
        lines.append(f'  secrets_scope:\n    default: "{state["secrets_scope"]}"\n')
    for key, info in sorted(state.get("tiers", {}).items()):
        lines.append(f'  space_id_{key}:\n    default: "{info["space_id"]}"\n')
        lines.append(f'  warehouse_id_{key}:\n    default: "{info["warehouse_id"]}"\n')
        lines.append(f'  sp_application_id_{key}:\n    default: "{info["sp_application_id"]}"\n')
    out = REPO_ROOT / "resources" / "provisioned_ids.auto.yml"
    out.write_text("".join(lines))
    print(f"Wrote {out}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tiers", nargs="*", default=None)
    parser.add_argument("--tiers-csv", default=None, help="Comma-separated tiers, e.g. t0,t4,t16")
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--warehouse-size", default=None)
    parser.add_argument("--secrets-scope", default=None)
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    provision(
        _parse_tiers(args.tiers, args.tiers_csv),
        args.catalog or cfg["catalog"],
        args.schema or cfg["schema"],
        args.warehouse_size or cfg.get("warehouse", {}).get("size", "Medium"),
        cfg.get("warehouse", {}).get("auto_stop_mins", 10),
        args.secrets_scope or cfg.get("secrets_scope") or DEFAULT_SECRETS_SCOPE,
        channel=cfg.get("warehouse", {}).get("channel", "PREVIEW"),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
