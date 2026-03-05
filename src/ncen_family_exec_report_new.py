#!/usr/bin/env python3
"""Generate NCEN Family Executive Report (new) from Edgar Agents perspective."""

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

from weasyprint import HTML

ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "ncen_family_exec_report_new.sql"
OUTPUT_DIR = ROOT / "output"

EA_ALIASES = {
    "EDGAR AGENTS LLC",
    "EDGAR AGENTS, LLC",
    "EDGAR AGENTS",
}


def _normalized(value: str | None) -> str:
    return (value or "").strip()


def _is_effective_value(value: str) -> bool:
    lowered = value.strip().lower()
    return bool(lowered) and lowered not in {"(unset)", "unset", "none", "null"}


def _detect_default_project_id() -> str:
    env_project = _normalized(os.getenv("GOOGLE_CLOUD_PROJECT"))
    if _is_effective_value(env_project):
        return env_project
    result = subprocess.run(["gcloud", "config", "get-value", "project", "--quiet"], check=False, capture_output=True, text=True)
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


def _display(value: Any) -> str:
    return "" if value is None else str(value)


def _split_entities(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts = re.split(r"[;|\n]+", text)
    cleaned: list[str] = []
    for part in parts:
        name = re.sub(r"\s+", " ", part).strip(" ,")
        if name:
            cleaned.append(name)
    return sorted(set(cleaned))


def _parse_other_agents(value: Any) -> list[str]:
    normalized_agents = _split_entities(value)
    out = []
    for agent in normalized_agents:
        upper = agent.upper()
        if upper not in EA_ALIASES:
            out.append(agent)
    return sorted(set(out))


def _family_rollups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    families: dict[str, dict[str, Any]] = {}
    for row in rows:
        family = _display(row.get("ncen_family_investment_company_name")).strip()
        if not family:
            continue
        slot = families.setdefault(
            family,
            {
                "family": family,
                "registrants": set(),
                "admins": set(),
                "advisers": set(),
                "other_agents": set(),
                "total_filings": 0,
                "qes_filings": 0,
            },
        )

        registrant = _display(row.get("companyName")).strip()
        if registrant:
            slot["registrants"].add(registrant)
        slot["admins"].update(_split_entities(row.get("ncen_admin_names")))
        slot["advisers"].update(_split_entities(row.get("ncen_adviser_names")))
        slot["other_agents"].update(_parse_other_agents(row.get("agent_groups_used_in_window")))
        slot["total_filings"] += int(row.get("total_filings_in_window") or 0)
        slot["qes_filings"] += int(row.get("qes_filings_in_window") or 0)

    out: list[dict[str, Any]] = []
    for item in families.values():
        total_filings = int(item["total_filings"])
        qes_filings = int(item["qes_filings"])
        qes_share = round((qes_filings / total_filings * 100.0), 2) if total_filings else 0.0
        out.append(
            {
                "family": item["family"],
                "fund_count": len(item["registrants"]),
                "admin_count": len(item["admins"]),
                "adviser_count": len(item["advisers"]),
                "other_agent_count": len(item["other_agents"]),
                "total_filings": total_filings,
                "qes_filings": qes_filings,
                "qes_share_pct": qes_share,
                "admins": "; ".join(sorted(item["admins"])),
                "advisers": "; ".join(sorted(item["advisers"])),
                "other_agents": "; ".join(sorted(item["other_agents"])),
            }
        )

    return sorted(out, key=lambda x: (-x["fund_count"], -x["other_agent_count"], x["family"]))


def _relationship_rollup(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rollup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        family = _display(row.get("ncen_family_investment_company_name")).strip()
        registrant = _display(row.get("companyName")).strip()
        other_agents = _parse_other_agents(row.get("agent_groups_used_in_window"))
        for admin in _split_entities(row.get("ncen_admin_names")):
            for adviser in _split_entities(row.get("ncen_adviser_names")):
                key = (admin, adviser)
                slot = rollup.setdefault(
                    key,
                    {
                        "admin": admin,
                        "adviser": adviser,
                        "families": set(),
                        "registrants": set(),
                        "other_agents": set(),
                    },
                )
                if family:
                    slot["families"].add(family)
                if registrant:
                    slot["registrants"].add(registrant)
                slot["other_agents"].update(other_agents)

    out = []
    for item in rollup.values():
        out.append(
            {
                "admin": item["admin"],
                "adviser": item["adviser"],
                "family_count": len(item["families"]),
                "registrant_count": len(item["registrants"]),
                "other_agent_count": len(item["other_agents"]),
                "families": "; ".join(sorted(item["families"])),
                "other_agents": "; ".join(sorted(item["other_agents"])),
            }
        )
    return sorted(out, key=lambda x: (-x["family_count"], -x["other_agent_count"], x["admin"], x["adviser"]))


def _table(headers: list[str], rows: list[list[str]]) -> str:
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    trs = []
    for row in rows:
        tds = "".join(f"<td>{html.escape(c)}</td>" for c in row)
        trs.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(trs)}</tbody></table>"


def render_report(rows: list[dict[str, Any]], output_pdf: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    families = _family_rollups(rows)
    relationships = _relationship_rollup(rows)

    family_table_rows = [
        [
            r["family"],
            str(r["fund_count"]),
            str(r["admin_count"]),
            str(r["adviser_count"]),
            str(r["other_agent_count"]),
            str(r["total_filings"]),
            str(r["qes_filings"]),
            f"{r['qes_share_pct']}%",
            r["other_agents"],
        ]
        for r in families
    ]

    relationship_rows = [
        [
            r["admin"],
            r["adviser"],
            str(r["family_count"]),
            str(r["registrant_count"]),
            str(r["other_agent_count"]),
            r["families"],
            r["other_agents"],
        ]
        for r in relationships[:150]
    ]

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <style>
    @page {{ size: Letter landscape; margin: 0.35in; }}
    body {{ font-family: Arial, sans-serif; font-size: 10px; color: #1c1c1c; }}
    h1 {{ font-size: 21px; margin: 0 0 8px 0; border-bottom: 2px solid #0c4a6e; padding-bottom: 4px; }}
    h2 {{ font-size: 14px; margin: 10px 0 6px 0; color: #334155; }}
    .note {{ margin: 0 0 10px 0; color: #475569; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
    th, td {{ border: 1px solid #d1d5db; padding: 4px; vertical-align: top; word-wrap: break-word; }}
    th {{ background: #f1f5f9; text-align: left; }}
    .page-break {{ page-break-before: always; }}
  </style>
</head>
<body>
  <h1>NCEN Family Executive Report (new) — EA Perspective</h1>
  <p class=\"note\">Scope: families where Edgar Agents has filing activity. Includes family-level coverage, non-EA filing agent overlap, and admin/adviser relationship maps.</p>
  <h2>Family Coverage (EA Accounts)</h2>
  {_table(["Family", "Funds", "Admins", "Advisers", "Other Agents", "Total Filings", "QES Filings", "QES %", "Other Filing Agents"], family_table_rows)}

  <div class=\"page-break\"></div>
  <h2>Admin/Adviser Relationship Map (Top 150 by family count)</h2>
  {_table(["Admin", "Adviser", "Families", "Registrants", "Other Agents", "Families List", "Other Filing Agents"], relationship_rows)}
</body>
</html>
"""

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html_doc).write_pdf(str(output_pdf))
    return families, relationships


def write_csv(rows: list[dict[str, Any]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_csv.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate NCEN Family Executive Report (new) from EA perspective.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "ncen_family_exec_report_new.pdf"),
        help="PDF output path",
    )
    args = parser.parse_args()

    project_id = _normalized(args.project_id)
    if not _is_effective_value(project_id):
        project_id = _detect_default_project_id()
    if not _is_effective_value(project_id):
        raise SystemExit("BigQuery project is empty. Set --project-id, BQ_PROJECT_ID, or GOOGLE_CLOUD_PROJECT.")

    rows = query_rows(project_id=project_id)
    family_rows, relationship_rows = render_report(rows, Path(args.output))

    family_csv = Path(args.output).with_suffix("").with_name(Path(args.output).stem + "_families.csv")
    relationship_csv = Path(args.output).with_suffix("").with_name(Path(args.output).stem + "_admin_adviser.csv")
    write_csv(family_rows, family_csv)
    write_csv(relationship_rows, relationship_csv)

    print(f"Created report: {args.output}")
    print(f"Created family rollup CSV: {family_csv}")
    print(f"Created admin/adviser rollup CSV: {relationship_csv}")


if __name__ == "__main__":
    main()
