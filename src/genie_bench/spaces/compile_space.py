"""Compile tier YAML → Genie serialized_space v2 JSON (API wire format).

Wire format grounded against:
  - Official Genie Agents API docs (serialized_space v2)
  - src/genie_bench/spaces/wire_samples/canonical_v2_full.json
  - Live export: wire_samples/t4_live_export.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import warnings
from pathlib import Path
from typing import Any

from genie_bench.config_utils import CONFIG_DIR, REPO_ROOT, load_benchmark_config, render_template

TIERS_DIR = CONFIG_DIR / "tiers"
OUT_DIR = REPO_ROOT / "src" / "genie_bench" / "spaces" / "compiled"

# Documented Genie agent limits
MAX_TABLES = 30
MAX_INSTRUCTIONS = 100  # each example SQL + each SQL function + text block = 1
MAX_KS_SNIPPETS = 200  # table descriptions + joins + SQL expressions

_RT_MAP = {
    "many_to_one": "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",
    "one_to_many": "FROM_RELATIONSHIP_TYPE_ONE_TO_MANY",
    "one_to_one": "FROM_RELATIONSHIP_TYPE_ONE_TO_ONE",
    "many_to_many": "FROM_RELATIONSHIP_TYPE_MANY_TO_MANY",
    "MANY_TO_ONE": "FROM_RELATIONSHIP_TYPE_MANY_TO_ONE",
    "ONE_TO_MANY": "FROM_RELATIONSHIP_TYPE_ONE_TO_MANY",
    "ONE_TO_ONE": "FROM_RELATIONSHIP_TYPE_ONE_TO_ONE",
    "MANY_TO_MANY": "FROM_RELATIONSHIP_TYPE_MANY_TO_MANY",
}


def _id(n: int) -> str:
    """Stable 32-hex id (sortable within a tier when n increases)."""
    return f"{n:032x}"


def _chunks(text: str) -> list[str]:
    """Split text into line chunks as Genie export does."""
    if not text:
        return []
    lines = text.splitlines(keepends=True)
    if not lines:
        return [text]
    # Ensure trailing newline so array concat doesn't jam tokens
    if lines and not lines[-1].endswith("\n"):
        lines[-1] = lines[-1] + "\n"
    return lines


def _as_list_str(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x) for x in val]
    return [str(val)]


def _read_sql_file(rel: str, mapping: dict[str, str]) -> str:
    path = REPO_ROOT / rel
    return render_template(path.read_text(), mapping).strip()


def _alias_from_ident(ident: str) -> str:
    name = ident.split(".")[-1]
    # short stable alias
    parts = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").split("_")
    if len(parts) == 1:
        return parts[0][:12].lower()
    return "".join(p[0] for p in parts if p)[:12].lower() or "t"


def _is_metric_view(ident: str) -> bool:
    leaf = ident.split(".")[-1].lower()
    return leaf.startswith("mv_") or ".mv_" in ident.lower()


def _compile_column_configs(cfgs: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in cfgs or []:
        entry: dict[str, Any] = {"column_name": c["column_name"]}
        if c.get("description"):
            entry["description"] = _as_list_str(c["description"])
        if c.get("synonyms"):
            entry["synonyms"] = list(c["synonyms"])
        if c.get("exclude") is not None:
            entry["exclude"] = bool(c["exclude"])
        # v2 prompt matching field names (NOT v1 get_example_values / build_value_dictionary)
        fmt = c.get("enable_format_assistance", c.get("format_assistance"))
        ent = c.get("enable_entity_matching", c.get("entity_matching"))
        if fmt is not None:
            entry["enable_format_assistance"] = bool(fmt)
        if ent is not None:
            entry["enable_entity_matching"] = bool(ent)
            if entry.get("enable_entity_matching") and not entry.get("enable_format_assistance"):
                entry["enable_format_assistance"] = True
        out.append(entry)
    out.sort(key=lambda x: x["column_name"])
    return out


def _compile_data_sources(tier: dict[str, Any]) -> dict[str, Any]:
    tables: list[dict[str, Any]] = []
    metric_views: list[dict[str, Any]] = []

    # Optional per-object overrides keyed by identifier (or leaf name)
    obj_meta: dict[str, Any] = {}
    for item in tier.get("data_object_meta", []) or []:
        key = item.get("identifier") or item.get("name")
        if key:
            obj_meta[key] = item
            obj_meta[key.split(".")[-1]] = item

    # Global column_configs may be a map: table_leaf -> [configs] OR flat list under objects
    col_map = tier.get("column_configs") or {}
    if isinstance(col_map, list):
        # legacy empty list / unused
        col_map = {}

    for ident in sorted(tier.get("data_objects", []) or []):
        meta = obj_meta.get(ident) or obj_meta.get(ident.split(".")[-1]) or {}
        entry: dict[str, Any] = {"identifier": ident}
        desc = meta.get("description") or tier.get("table_descriptions", {}).get(ident)
        if not desc and isinstance(tier.get("table_descriptions"), dict):
            desc = tier["table_descriptions"].get(ident.split(".")[-1])
        if desc:
            entry["description"] = _as_list_str(desc)

        cfgs = meta.get("column_configs")
        if cfgs is None and isinstance(col_map, dict):
            cfgs = col_map.get(ident) or col_map.get(ident.split(".")[-1])
        compiled = _compile_column_configs(cfgs)
        if compiled:
            entry["column_configs"] = compiled

        if meta.get("is_metric_view") or _is_metric_view(ident):
            metric_views.append(entry)
        else:
            tables.append(entry)

    ds: dict[str, Any] = {}
    if tables:
        ds["tables"] = tables
    if metric_views:
        ds["metric_views"] = metric_views
    return ds


def _compile_join_specs(joins: list[dict[str, Any]] | None, start_id: int) -> list[dict[str, Any]]:
    out = []
    for i, js in enumerate(joins or [], start=1):
        left_ident = js.get("left") or js.get("left_table_identifier")
        right_ident = js.get("right") or js.get("right_table_identifier")
        if isinstance(left_ident, dict):
            left = {
                "identifier": left_ident["identifier"],
                "alias": left_ident.get("alias") or _alias_from_ident(left_ident["identifier"]),
            }
        else:
            left = {"identifier": left_ident, "alias": js.get("left_alias") or _alias_from_ident(left_ident)}
        if isinstance(right_ident, dict):
            right = {
                "identifier": right_ident["identifier"],
                "alias": right_ident.get("alias") or _alias_from_ident(right_ident["identifier"]),
            }
        else:
            right = {
                "identifier": right_ident,
                "alias": js.get("right_alias") or _alias_from_ident(right_ident),
            }

        cond = js.get("sql") or js.get("condition") or ""
        if isinstance(cond, list):
            # already wire-shaped
            sql_arr = [str(x) for x in cond]
            if len(sql_arr) == 1 or not any(x.startswith("--rt=") for x in sql_arr):
                rt = js.get("relationship_type") or js.get("relationship") or "many_to_one"
                rt_ann = f"--rt={_RT_MAP.get(rt, _RT_MAP['many_to_one'])}--"
                # normalize condition to backtick-quoted aliases if bare
                sql_arr = [sql_arr[0], rt_ann]
        else:
            cond_s = str(cond).strip()
            # Allow shorthand "geo_key" → `left`.`geo_key` = `right`.`geo_key`
            if cond_s and "=" not in cond_s and " " not in cond_s:
                cond_s = f"`{left['alias']}`.`{cond_s}` = `{right['alias']}`.`{cond_s}`"
            elif cond_s and "--rt=" not in cond_s:
                # Ensure backtick aliases when condition uses alias.col form
                pass
            rt = js.get("relationship_type") or js.get("relationship") or "many_to_one"
            rt_ann = f"--rt={_RT_MAP.get(rt, _RT_MAP['many_to_one'])}--"
            sql_arr = [cond_s, rt_ann]

        entry: dict[str, Any] = {
            "id": _id(start_id + i),
            "left": left,
            "right": right,
            "sql": sql_arr,
        }
        if js.get("comment"):
            entry["comment"] = _as_list_str(js["comment"])
        if js.get("instruction") or js.get("usage"):
            entry["instruction"] = _as_list_str(js.get("instruction") or js.get("usage"))
        out.append(entry)
    out.sort(key=lambda x: x["id"])
    return out


def _compile_snippets(snippets: dict[str, Any] | None, start_id: int) -> dict[str, Any]:
    snippets = snippets or {}
    out: dict[str, Any] = {}
    n = start_id

    def _one(kind: str, items: list[dict[str, Any]], require_alias: bool) -> list[dict[str, Any]]:
        nonlocal n
        compiled = []
        for item in items or []:
            n += 1
            entry: dict[str, Any] = {
                "id": _id(n),
                "sql": _chunks(item["sql"]) if "\n" in item.get("sql", "") else [item["sql"]],
            }
            if item.get("name") or item.get("display_name"):
                entry["display_name"] = item.get("display_name") or item["name"]
            if require_alias or item.get("alias"):
                alias = item.get("alias") or re.sub(r"[^a-z0-9]+", "_", (entry.get("display_name") or "expr").lower()).strip("_")
                entry["alias"] = alias
            if item.get("synonyms"):
                entry["synonyms"] = list(item["synonyms"])
            if item.get("usage") or item.get("instruction"):
                entry["instruction"] = _as_list_str(item.get("instruction") or item.get("usage"))
            if item.get("comment"):
                entry["comment"] = _as_list_str(item["comment"])
            compiled.append(entry)
        compiled.sort(key=lambda x: x["id"])
        return compiled

    # Accept both "fields" (our YAML) and "expressions" (wire)
    filters = _one("filters", snippets.get("filters") or [], require_alias=False)
    expressions = _one(
        "expressions",
        snippets.get("expressions") or snippets.get("fields") or [],
        require_alias=True,
    )
    measures = _one("measures", snippets.get("measures") or [], require_alias=True)
    if filters:
        out["filters"] = filters
    if expressions:
        out["expressions"] = expressions
    if measures:
        out["measures"] = measures
    return out


def _compile_examples(examples: list[dict[str, Any]] | None, mapping: dict[str, str], start_id: int) -> list[dict[str, Any]]:
    out = []
    for i, ex in enumerate(examples or [], start=1):
        if "sql_file" in ex:
            sql = _read_sql_file(ex["sql_file"], mapping)
        else:
            sql = render_template(ex.get("sql", ""), mapping)
        title = ex.get("title") or ex.get("question", "")
        entry: dict[str, Any] = {
            "id": _id(start_id + i),
            "question": [title],
            "sql": _chunks(sql + ("\n" if sql and not sql.endswith("\n") else "")),
        }
        if ex.get("usage") or ex.get("usage_guidance"):
            entry["usage_guidance"] = _as_list_str(ex.get("usage_guidance") or ex.get("usage"))
        params = ex.get("parameters") or []
        if params:
            wire_params = []
            for p in params:
                wp: dict[str, Any] = {"name": p["name"]}
                if p.get("type_hint") or p.get("type"):
                    wp["type_hint"] = p.get("type_hint") or p.get("type")
                if p.get("description"):
                    wp["description"] = _as_list_str(p["description"])
                if p.get("default") is not None or p.get("default_value") is not None:
                    default = p.get("default_value") or p.get("default")
                    if isinstance(default, dict) and "values" in default:
                        wp["default_value"] = default
                    else:
                        wp["default_value"] = {"values": [str(default)]}
                wire_params.append(wp)
            entry["parameters"] = wire_params
        out.append(entry)
    out.sort(key=lambda x: x["id"])
    return out


def _compile_sql_functions(funcs: list[dict[str, Any]] | None, mapping: dict[str, str], start_id: int) -> list[dict[str, Any]]:
    out = []
    for i, f in enumerate(funcs or [], start=1):
        ident = render_template(f["identifier"], mapping) if "${" in f.get("identifier", "") else f["identifier"]
        # Allow shorthand leaf name → catalog.schema.leaf
        if ident.count(".") == 0:
            ident = f"{mapping['catalog']}.{mapping['schema']}.{ident}"
        entry: dict[str, Any] = {
            "id": _id(start_id + i),
            "identifier": ident,
        }
        # NOTE: some workspaces reject sql_functions[].description — omit from wire payload.
        # Keep description only in tier YAML / docs for humans.
        out.append(entry)
    out.sort(key=lambda x: (x["id"], x["identifier"]))
    return out


def validate_limits(serialized: dict[str, Any], tier_code: str) -> list[str]:
    warns: list[str] = []
    ds = serialized.get("data_sources") or {}
    n_tables = len(ds.get("tables") or []) + len(ds.get("metric_views") or [])
    if n_tables > MAX_TABLES:
        warns.append(f"{tier_code}: {n_tables} tables/MVs exceeds limit {MAX_TABLES}")

    instr = serialized.get("instructions") or {}
    n_instr = 0
    if instr.get("text_instructions"):
        n_instr += len(instr["text_instructions"])
    n_instr += len(instr.get("example_question_sqls") or [])
    n_instr += len(instr.get("sql_functions") or [])
    if n_instr > MAX_INSTRUCTIONS:
        warns.append(f"{tier_code}: {n_instr} instructions exceeds limit {MAX_INSTRUCTIONS}")

    # Knowledge-store snippets ≈ table descriptions + joins + SQL expressions
    n_ks = 0
    for t in (ds.get("tables") or []) + (ds.get("metric_views") or []):
        if t.get("description"):
            n_ks += 1
        n_ks += len(t.get("column_configs") or [])
    n_ks += len(instr.get("join_specs") or [])
    snippets = instr.get("sql_snippets") or {}
    for k in ("filters", "expressions", "measures"):
        n_ks += len(snippets.get(k) or [])
    if n_ks > MAX_KS_SNIPPETS:
        warns.append(f"{tier_code}: {n_ks} knowledge-store snippets exceeds limit {MAX_KS_SNIPPETS}")
    return warns


def compile_tier(tier_path: Path, catalog: str, schema: str) -> dict[str, Any]:
    import yaml

    mapping = {"catalog": catalog, "schema": schema}
    rendered = render_template(tier_path.read_text(), mapping)
    tier = yaml.safe_load(rendered)

    data_sources = _compile_data_sources(tier)

    sample_questions = []
    for i, q in enumerate(tier.get("sample_questions", []) or [], start=1):
        sample_questions.append({"id": _id(0x100 + i), "question": [q] if isinstance(q, str) else _as_list_str(q)})
    sample_questions.sort(key=lambda x: x["id"])

    text = (tier.get("instructions", {}) or {}).get("text", "") or ""
    text_instructions = []
    if text.strip():
        text_instructions.append({"id": _id(0x200), "content": _chunks(text)})

    instr_block = tier.get("instructions", {}) or {}
    raw_examples = list(instr_block.get("example_sqls") or [])
    # Optional stress: generate many low-value examples (instruction-limit probe)
    overload_n = int(instr_block.get("example_sqls_overload") or 0)
    if overload_n > 0:
        base_sql = (
            f"SELECT COUNT(*) AS n FROM {catalog}.{schema}.fact_order_line "
            f"WHERE line_status = 'FULFILLED'"
        )
        for i in range(overload_n):
            raw_examples.append(
                {
                    "title": f"Count fulfilled order lines variant {i+1:03d}",
                    "sql": base_sql + (f" /* variant {i+1} */" if i else ""),
                    "usage": "Low-value overload example for instruction-limit stress test",
                }
            )
    example_sqls = _compile_examples(raw_examples, mapping, 0x210)
    join_specs = _compile_join_specs(instr_block.get("join_specs") or [], 0x300)
    snippets = _compile_snippets(instr_block.get("sql_snippets") or {}, 0x400)
    sql_functions = _compile_sql_functions(
        instr_block.get("sql_functions") or tier.get("sql_functions") or [],
        mapping,
        0x550,
    )

    benchmark_questions = []
    for i, b in enumerate(tier.get("benchmarks", []) or [], start=1):
        sql = _read_sql_file(b["sql_file"], mapping) if "sql_file" in b else b.get("sql", "")
        benchmark_questions.append(
            {
                "id": _id(0x600 + i),
                "question": [b["question"]],
                "answer": [
                    {
                        "format": "SQL",
                        "content": _chunks(sql + ("\n" if sql and not sql.endswith("\n") else "")),
                    }
                ],
            }
        )
    benchmark_questions.sort(key=lambda x: x["id"])

    instructions: dict[str, Any] = {}
    if text_instructions:
        instructions["text_instructions"] = text_instructions
    if example_sqls:
        instructions["example_question_sqls"] = example_sqls
    if sql_functions:
        instructions["sql_functions"] = sql_functions
    if join_specs:
        instructions["join_specs"] = join_specs
    if snippets:
        instructions["sql_snippets"] = snippets

    serialized: dict[str, Any] = {
        "version": 2,
        "data_sources": data_sources or {"tables": []},
    }
    if sample_questions:
        serialized["config"] = {"sample_questions": sample_questions}
    if instructions:
        serialized["instructions"] = instructions
    if benchmark_questions:
        serialized["benchmarks"] = {"questions": benchmark_questions}

    for wmsg in validate_limits(serialized, tier.get("tier", tier_path.stem)):
        warnings.warn(wmsg, stacklevel=2)
        print(f"WARN: {wmsg}")

    meta = {
        "tier": tier["tier"],
        "name": tier["name"],
        "axis": tier.get("axis", "unspecified"),
        "lever": tier.get("lever", tier.get("name")),
        "intent": tier.get("intent", ""),
        "display_name": f"Genie TCO {tier['tier']} — {tier['name']}",
        "description": tier.get("description", "") or "",
        "manual_steps": tier.get("manual_steps", []) or [],
        "serialized_space": serialized,
    }
    return meta


def compile_all(catalog: str, schema: str, tiers: list[str] | None = None) -> list[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    wanted = None
    if tiers:
        wanted = {t.lower() if t.lower().startswith("t") else f"t{t.lower()}" for t in tiers}
        wanted |= {t.lower() for t in tiers}
    for path in sorted(TIERS_DIR.glob("t*.yaml")):
        if wanted is not None and path.stem.lower() not in wanted:
            continue
        meta = compile_tier(path, catalog, schema)
        out = OUT_DIR / f"{path.stem}.serialized.json"
        out.write_text(json.dumps(meta, indent=2))
        written.append(out)
        print(f"Compiled {meta['tier']} axis={meta.get('axis')} -> {out}")
    return written


def _parse_tiers_arg(tiers: list[str] | None, tiers_csv: str | None) -> list[str] | None:
    if tiers_csv:
        return [t.strip().lower() for t in tiers_csv.split(",") if t.strip()]
    if tiers:
        return [t.lower() for t in tiers]
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tiers", nargs="*", default=None, help="e.g. t0 t4")
    parser.add_argument("--tiers-csv", default=None, help="Comma-separated tiers, e.g. t0,t4,t16")
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    args = parser.parse_args(argv)
    cfg = load_benchmark_config()
    compile_all(
        args.catalog or cfg["catalog"],
        args.schema or cfg["schema"],
        _parse_tiers_arg(args.tiers, args.tiers_csv),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
