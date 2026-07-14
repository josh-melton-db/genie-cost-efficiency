"""Build WorkspaceClient authenticated as a tier service principal (OAuth M2M)."""

from __future__ import annotations

import base64
from typing import Any

from databricks.sdk import WorkspaceClient


def workspace_client_for_tier(
    admin_w: WorkspaceClient,
    tier_info: dict[str, Any],
    *,
    fallback_to_admin: bool = True,
) -> tuple[WorkspaceClient, str]:
    """Return (client, run_as_identity).

    Prefers SP OAuth secrets stored in the secret scope by provision_spaces.py.
    Falls back to the admin/user client when secrets are missing (prove-out mode).
    """
    oauth = tier_info.get("oauth") or {}
    scope = oauth.get("secrets_scope") or "genie-tco-bench"
    id_key = oauth.get("client_id_key")
    secret_key = oauth.get("client_secret_key")
    app_id = tier_info.get("sp_application_id")

    if not id_key or not secret_key or not app_id:
        if fallback_to_admin:
            return admin_w, f"admin_fallback:{app_id or 'unknown'}"
        raise RuntimeError(f"Missing OAuth metadata for tier {tier_info.get('tier')}")

    try:
        # SDK returns Secret with bytes value (base64-encoded in API)
        client_id_secret = admin_w.secrets.get_secret(scope=scope, key=id_key)
        client_secret_secret = admin_w.secrets.get_secret(scope=scope, key=secret_key)

        def _decode(val) -> str:
            if val is None:
                return ""
            if isinstance(val, bytes):
                # Databricks secrets API returns base64-encoded bytes
                try:
                    return base64.b64decode(val).decode()
                except Exception:
                    return val.decode()
            s = str(val)
            try:
                return base64.b64decode(s).decode()
            except Exception:
                return s

        client_id = _decode(getattr(client_id_secret, "value", None) or client_id_secret)
        client_secret = _decode(getattr(client_secret_secret, "value", None) or client_secret_secret)
        if not client_id:
            client_id = app_id

        host = admin_w.config.host
        sp_w = WorkspaceClient(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
            auth_type="oauth-m2m",
        )
        # Prefer a lightweight Genie call over SCIM /Me (needs entitlements; Genie is the real target).
        try:
            sp_w.api_client.do("GET", f"/api/2.0/genie/spaces/{tier_info['space_id']}")
        except Exception as smoke_err:  # noqa: BLE001
            # Still return the client — runner will surface Genie errors per question
            print(f"SP Genie smoke warning ({tier_info.get('tier')}): {smoke_err}")
        print(f"SP client ready for {tier_info.get('tier')}: {app_id}")
        return sp_w, app_id
    except Exception as e:  # noqa: BLE001
        print(f"SP OAuth client failed ({tier_info.get('tier')}): {e}")
        if fallback_to_admin:
            return admin_w, f"admin_fallback:{app_id}"
        raise
