#!/usr/bin/env python3
"""Generate NCEN relationship network report focused on admin/adviser overlap leverage."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "ncen_relationship_network_report.sql"
OUTPUT_DIR = ROOT / "output"


def _normalized(value: str | None) -> str:
    return (value or "").strip()


def _is_effective_value(value: str) -> bool:
    lowered = value.strip().lower()
    return bool(lowered) and lowered not in {"(unset)", "unset", "none", "null"}


def _detect_default_project_id() -> str:
    env_project = _normalized(os.getenv("GOOGLE_CLOUD_PROJECT"))
    if _is_effective_value(env_project):
        return env_project
    cmd = ["gcloud", "config", "get-value", "project", "--quiet"]
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode == 0:
        detected = _normalized(result.stdout)
        if _is_effective_value(detected):
            return detected
    return ""


def query_rows(project_id: str) -> list[dict[str, Any]]:
    sql = SQL_PATH.read_text(encoding="utf-8")
    cmd = [
        "bq",
        "query",
        "--project_id",
        project_id,
        "--use_legacy_sql=false",
        "--format=prettyjson",
        "--max_rows=1000000",
    ]
    result = subprocess.run(cmd, input=sql, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "No output from bq CLI"
        raise RuntimeError(f"bq query failed (exit {result.returncode}): {detail}")
    payload = result.stdout.strip()
    if not payload:
        return []
    data = json.loads(payload)
    if isinstance(data, list):
        return [dict(row) for row in data]
    raise RuntimeError("Unexpected bq output format. Expected JSON array.")


def _split_entities(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts = re.split(r"[;|\n]+", text)
    cleaned = []
    for part in parts:
        name = re.sub(r"\s+", " ", part).strip(" ,")
        if name:
            cleaned.append(name)
    return sorted(set(cleaned))


def _is_true(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _rollup_by_entity(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    acc: dict[str, dict[str, Any]] = {}
    for row in rows:
        entities = _split_entities(row.get(key))
        family = str(row.get("ncen_family_investment_company_name") or "").strip()
        registrant = str(row.get("companyName") or "").strip()
        uses_qes = float(row.get("qes_filings_in_window") or 0) > 0
        uses_ea = _is_true(row.get("ever_filed_by_edgar_agents_llc_in_window"))

        for entity in entities:
            slot = acc.setdefault(
                entity,
                {
                    "entity": entity,
                    "registrants": set(),
                    "families": set(),
                    "ea_registrants": set(),
                    "qes_registrants": set(),
                    "both_registrants": set(),
                    "ea_families": set(),
                    "qes_families": set(),
                    "both_families": set(),
                },
            )
            if registrant:
                slot["registrants"].add(registrant)
                if uses_ea:
                    slot["ea_registrants"].add(registrant)
                if uses_qes:
                    slot["qes_registrants"].add(registrant)
                if uses_ea and uses_qes:
                    slot["both_registrants"].add(registrant)
            if family:
                slot["families"].add(family)
                if uses_ea:
                    slot["ea_families"].add(family)
                if uses_qes:
                    slot["qes_families"].add(family)
                if uses_ea and uses_qes:
                    slot["both_families"].add(family)

    out = []
    for item in acc.values():
        out.append(
            {
                "entity": item["entity"],
                "registrant_count": len(item["registrants"]),
                "family_count": len(item["families"]),
                "ea_registrant_count": len(item["ea_registrants"]),
                "qes_registrant_count": len(item["qes_registrants"]),
                "both_ea_qes_registrant_count": len(item["both_registrants"]),
                "ea_family_count": len(item["ea_families"]),
                "qes_family_count": len(item["qes_families"]),
                "both_family_count": len(item["both_families"]),
                "families": "; ".join(sorted(item["families"])),
                "ea_families": "; ".join(sorted(item["ea_families"])),
                "qes_families": "; ".join(sorted(item["qes_families"])),
                "both_families": "; ".join(sorted(item["both_families"])),
            }
        )
    return sorted(
        out,
        key=lambda x: (
            -x["both_ea_qes_registrant_count"],
            -x["ea_registrant_count"],
            -x["qes_registrant_count"],
            x["entity"],
        ),
    )


def _rollup_admin_adviser_pairs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    acc: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        admins = _split_entities(row.get("ncen_admin_names"))
        advisers = _split_entities(row.get("ncen_adviser_names"))
        family = str(row.get("ncen_family_investment_company_name") or "").strip()
        registrant = str(row.get("companyName") or "").strip()
        uses_qes = float(row.get("qes_filings_in_window") or 0) > 0
        uses_ea = _is_true(row.get("ever_filed_by_edgar_agents_llc_in_window"))

        for admin in admins:
            for adviser in advisers:
                key = (admin, adviser)
                slot = acc.setdefault(
                    key,
                    {
                        "admin": admin,
                        "adviser": adviser,
                        "families": set(),
                        "ea_families": set(),
                        "qes_families": set(),
                        "both_families": set(),
                        "registrants": set(),
                        "both_registrants": set(),
                    },
                )
                if family:
                    slot["families"].add(family)
                    if uses_ea:
                        slot["ea_families"].add(family)
                    if uses_qes:
                        slot["qes_families"].add(family)
                    if uses_ea and uses_qes:
                        slot["both_families"].add(family)
                if registrant:
                    slot["registrants"].add(registrant)
                    if uses_ea and uses_qes:
                        slot["both_registrants"].add(registrant)

    out = []
    for item in acc.values():
        out.append(
            {
                "admin": item["admin"],
                "adviser": item["adviser"],
                "family_count": len(item["families"]),
                "ea_family_count": len(item["ea_families"]),
                "qes_family_count": len(item["qes_families"]),
                "both_family_count": len(item["both_families"]),
                "registrant_count": len(item["registrants"]),
                "both_ea_qes_registrant_count": len(item["both_registrants"]),
                "families": "; ".join(sorted(item["families"])),
                "ea_families": "; ".join(sorted(item["ea_families"])),
                "qes_families": "; ".join(sorted(item["qes_families"])),
                "both_families": "; ".join(sorted(item["both_families"])),
            }
        )
    return sorted(
        out,
        key=lambda x: (
            -x["both_ea_qes_registrant_count"],
            -x["family_count"],
            -x["registrant_count"],
            x["admin"],
            x["adviser"],
        ),
    )


def _build_leverage_candidates(admin_rollup: list[dict[str, Any]], adviser_rollup: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = []
    for kind, rows in (("Admin", admin_rollup), ("Adviser", adviser_rollup)):
        for row in rows:
            ea_count = int(row["ea_registrant_count"])
            qes_count = int(row["qes_registrant_count"])
            both_count = int(row["both_ea_qes_registrant_count"])
            if both_count <= 0:
                continue
            whitespace_safe_families = row.get("families", "")
            candidates.append(
                {
                    "relationship_type": kind,
                    "name": row["entity"],
                    "both_ea_qes_registrant_count": both_count,
                    "ea_registrant_count": ea_count,
                    "qes_registrant_count": qes_count,
                    "expandable_qes_only_registrants": max(qes_count - both_count, 0),
                    "family_count": row["family_count"],
                    "families": whitespace_safe_families,
                    "ea_families": row.get("ea_families", ""),
                    "qes_families": row.get("qes_families", ""),
                }
            )
    return sorted(
        candidates,
        key=lambda x: (
            -x["expandable_qes_only_registrants"],
            -x["both_ea_qes_registrant_count"],
            -x["family_count"],
            x["relationship_type"],
            x["name"],
        ),
    )


def _table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body or '<tr><td colspan=\"99\">No rows</td></tr>'}</tbody></table>"


def render_report(rows: list[dict[str, Any]], output_pdf: Path) -> dict[str, list[dict[str, Any]]]:
    from weasyprint import HTML

    admin_rollup = _rollup_by_entity(rows, "ncen_admin_names")
    adviser_rollup = _rollup_by_entity(rows, "ncen_adviser_names")
    pair_rollup = _rollup_admin_adviser_pairs(rows)
    leverage = _build_leverage_candidates(admin_rollup, adviser_rollup)

    admin_table = _table(
        ["Admin", "Registrants", "Families", "EA", "QES", "Both EA+QES", "EA Family Names", "QES Family Names"],
        [
            [
                r["entity"],
                str(r["registrant_count"]),
                str(r["family_count"]),
                str(r["ea_registrant_count"]),
                str(r["qes_registrant_count"]),
                str(r["both_ea_qes_registrant_count"]),
                r["ea_families"],
                r["qes_families"],
            ]
            for r in admin_rollup[:200]
        ],
    )

    adviser_table = _table(
        ["Adviser", "Registrants", "Families", "EA", "QES", "Both EA+QES", "EA Family Names", "QES Family Names"],
        [
            [
                r["entity"],
                str(r["registrant_count"]),
                str(r["family_count"]),
                str(r["ea_registrant_count"]),
                str(r["qes_registrant_count"]),
                str(r["both_ea_qes_registrant_count"]),
                r["ea_families"],
                r["qes_families"],
            ]
            for r in adviser_rollup[:200]
        ],
    )

    pair_table = _table(
        ["Admin", "Adviser", "Families", "Registrants", "Both EA+QES", "EA Family Names", "QES Family Names"],
        [
            [
                r["admin"],
                r["adviser"],
                str(r["family_count"]),
                str(r["registrant_count"]),
                str(r["both_ea_qes_registrant_count"]),
                r["ea_families"],
                r["qes_families"],
            ]
            for r in pair_rollup[:250]
        ],
    )

    leverage_table = _table(
        ["Type", "Relationship Name", "Both EA+QES", "QES-only Expand", "Families", "EA Family Names", "QES Family Names"],
        [
            [
                r["relationship_type"],
                r["name"],
                str(r["both_ea_qes_registrant_count"]),
                str(r["expandable_qes_only_registrants"]),
                str(r["family_count"]),
                r["ea_families"],
                r["qes_families"],
            ]
            for r in leverage[:250]
        ],
    )

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <style>
    @page {{ size: Letter landscape; margin: 0.35in; }}
    body {{ font-family: Arial, sans-serif; font-size: 10px; color: #1c1c1c; }}
    h1 {{ font-size: 22px; margin: 0 0 8px 0; border-bottom: 2px solid #0c4a6e; padding-bottom: 5px; }}
    h2 {{ font-size: 14px; margin: 8px 0 6px 0; color: #334155; }}
    p {{ margin: 3px 0; }}
    table {{ width: 100%; border-collapse: collapse; margin: 6px 0 10px 0; table-layout: fixed; }}
    th, td {{ border: 1px solid #d1d5db; padding: 5px; vertical-align: top; word-wrap: break-word; }}
    th {{ background: #f1f5f9; text-align: left; }}
    section {{ page-break-after: always; }}
    section:last-child {{ page-break-after: auto; }}
  </style>
</head>
<body>
  <section>
    <h1>NCEN Relationship Network Report</h1>
    <p>Focus: roll up registrants by admin and adviser, then identify overlap where both EA and QES are present.</p>
    <p>Total registrants in scope: {len(rows)}</p>
    <p>Distinct admin entities: {len(admin_rollup)} | Distinct adviser entities: {len(adviser_rollup)} | Distinct admin+adviser pairs: {len(pair_rollup)}</p>
    <h2>Leverage Network (priority targets)</h2>
    {leverage_table}
  </section>
  <section>
    <h1>Registrant Rollup by Admin</h1>
    {admin_table}
  </section>
  <section>
    <h1>Registrant Rollup by Adviser</h1>
    {adviser_table}
  </section>
  <section>
    <h1>Family Network by Shared Admin + Adviser</h1>
    {pair_table}
  </section>
</body>
</html>
"""

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html_doc).write_pdf(str(output_pdf))
    return {
        "admin_rollup": admin_rollup,
        "adviser_rollup": adviser_rollup,
        "admin_adviser_pairs": pair_rollup,
        "leverage_network": leverage,
    }


def write_csv(rows: list[dict[str, Any]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_csv.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate NCEN relationship network report.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "ncen_relationship_network_report.pdf"),
        help="PDF output path",
    )
    args = parser.parse_args()

    project_id = _normalized(args.project_id)
    if not _is_effective_value(project_id):
        project_id = _detect_default_project_id()
    if not _is_effective_value(project_id):
        raise SystemExit("BigQuery project is empty. Set --project-id, BQ_PROJECT_ID, or GOOGLE_CLOUD_PROJECT.")

    rows = query_rows(project_id=project_id)
    sections = render_report(rows, Path(args.output))

    output_base = Path(args.output).with_suffix("")
    write_csv(sections["leverage_network"], output_base.with_name(output_base.name + "_leverage_network.csv"))
    write_csv(sections["admin_rollup"], output_base.with_name(output_base.name + "_by_admin.csv"))
    write_csv(sections["adviser_rollup"], output_base.with_name(output_base.name + "_by_adviser.csv"))
    write_csv(sections["admin_adviser_pairs"], output_base.with_name(output_base.name + "_admin_adviser_pairs.csv"))

    print(f"Created report: {args.output}")
    print(f"Created leverage data: {output_base.with_name(output_base.name + '_leverage_network.csv')}")


if __name__ == "__main__":
    main()
