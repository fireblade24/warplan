#!/usr/bin/env python3
"""Generate NCEN family-level executive report (one page per family) via WeasyPrint."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from weasyprint import HTML

ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "ncen_family_exec_report.sql"
OUTPUT_DIR = ROOT / "output"


@dataclass
class FamilyAiSummary:
    openness_to_switch: str
    potential_value_to_ea: str
    conversation_script: str
    reasoning: str


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


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def summarize_family(rows: list[dict[str, Any]]) -> FamilyAiSummary:
    funds = len(rows)
    total_filings = sum(_to_int(r.get("total_filings_in_window")) for r in rows)
    qes_filings = sum(_to_int(r.get("qes_filings_in_window")) for r in rows)
    qes_share = (qes_filings / total_filings * 100.0) if total_filings else 0.0
    avg_agent_count = (
        sum(_to_int(r.get("total_agent_groups_used_in_window")) for r in rows) / funds if funds else 0.0
    )

    value_score = 0
    value_score += 3 if total_filings >= 1000 else 2 if total_filings >= 300 else 1 if total_filings >= 100 else 0
    value_score += 2 if funds >= 10 else 1 if funds >= 4 else 0

    if value_score >= 5:
        potential_value = "$$$$"
    elif value_score >= 3:
        potential_value = "$$$"
    elif value_score >= 2:
        potential_value = "$$"
    else:
        potential_value = "$"

    switch_score = 0
    switch_score += 3 if qes_share < 25 else 2 if qes_share < 50 else 1 if qes_share < 70 else 0
    switch_score += 2 if avg_agent_count >= 4 else 1 if avg_agent_count >= 2 else 0

    if switch_score >= 5:
        openness = "Very High"
    elif switch_score >= 4:
        openness = "High"
    elif switch_score >= 3:
        openness = "Medium"
    elif switch_score >= 2:
        openness = "Low"
    else:
        openness = "Very Low"

    conversation_script = (
        "We support multi-fund filing operations with predictable execution, "
        "tight turnaround, and clean regulator-ready output. "
        "Could we review one recent high-volume filing cycle and identify where "
        "we can reduce touches and improve filing reliability across your funds?"
    )

    reasoning = (
        f"Funds={funds}, total filings={total_filings}, QES share={qes_share:.2f}%, "
        f"avg agent groups used={avg_agent_count:.2f}."
    )

    return FamilyAiSummary(
        openness_to_switch=openness,
        potential_value_to_ea=potential_value,
        conversation_script=conversation_script,
        reasoning=reasoning,
    )


def _display(value: Any) -> str:
    return "" if value is None else str(value)


def render_report(rows: list[dict[str, Any]], output_pdf: Path) -> list[dict[str, Any]]:
    families: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        fam = _display(row.get("ncen_family_investment_company_name")).strip()
        if not fam:
            continue
        families.setdefault(fam, []).append(row)

    pages = []
    flat_export: list[dict[str, Any]] = []

    for family_name in sorted(families.keys()):
        fund_rows = families[family_name]
        summary = summarize_family(fund_rows)

        fund_table_rows = []
        for r in fund_rows:
            fund_table_rows.append(
                "<tr>"
                f"<td>{html.escape(_display(r.get('companyName')))}</td>"
                f"<td>{html.escape(_display(r.get('companyCik')))}</td>"
                f"<td>{html.escape(_display(r.get('ncen_investment_company_type')))}</td>"
                f"<td>{html.escape(_display(r.get('ncen_total_series')))}</td>"
                f"<td>{html.escape(_display(r.get('ncen_accession_rows')))}</td>"
                f"<td>{html.escape(_display(r.get('ncen_admin_names')))}</td>"
                f"<td>{html.escape(_display(r.get('ncen_adviser_names')))}</td>"
                f"<td>{html.escape(_display(r.get('ncen_adviser_types')))}</td>"
                f"<td>{html.escape(_display(r.get('total_filings_in_window')))}</td>"
                f"<td>{html.escape(_display(r.get('qes_filings_in_window')))}</td>"
                f"<td>{html.escape(_display(r.get('qes_pct_of_company_filings_in_window')))}%</td>"
                f"<td>{html.escape(_display(r.get('ever_filed_by_edgar_agents_llc_in_window')))}</td>"
                f"<td>{html.escape(_display(r.get('total_agent_groups_used_in_window')))}</td>"
                f"<td>{html.escape(_display(r.get('agent_groups_used_in_window')))}</td>"
                "</tr>"
            )

            export_row = dict(r)
            export_row["family_openness_to_switch"] = summary.openness_to_switch
            export_row["family_potential_value_to_ea"] = summary.potential_value_to_ea
            export_row["family_conversation_script"] = summary.conversation_script
            export_row["family_ai_reasoning"] = summary.reasoning
            flat_export.append(export_row)

        page = f"""
<section class=\"family-page\">
  <h1>{html.escape(family_name)}</h1>
  <div class=\"summary\">
    <h2>AI Executive Summary</h2>
    <p><b>Openness to Switch:</b> {html.escape(summary.openness_to_switch)}</p>
    <p><b>Potential Value to EA:</b> {html.escape(summary.potential_value_to_ea)}</p>
    <p><b>Conversation Starter Script:</b> {html.escape(summary.conversation_script)}</p>
    <p><b>AI Reasoning:</b> {html.escape(summary.reasoning)}</p>
  </div>
  <h2>Funds in Family</h2>
  <table>
    <thead>
      <tr>
        <th>Fund</th><th>CIK</th><th>Investment Type</th><th>Total Series</th><th>Accession Rows</th>
        <th>Admin Names</th><th>Adviser Names</th><th>Adviser Types</th><th>Total Filings</th>
        <th>QES Filings</th><th>QES %</th><th>Filed by Edgar Agents LLC?</th><th>Total Agent Groups</th><th>Agent Groups Used</th>
      </tr>
    </thead>
    <tbody>
      {''.join(fund_table_rows)}
    </tbody>
  </table>
</section>
"""
        pages.append(page)

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
    .family-page {{ page-break-after: always; }}
    .family-page:last-child {{ page-break-after: auto; }}
    .summary {{ background: #f8fafc; border: 1px solid #cbd5e1; padding: 8px; margin-bottom: 10px; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
    th, td {{ border: 1px solid #d1d5db; padding: 5px; vertical-align: top; word-wrap: break-word; }}
    th {{ background: #e2e8f0; }}
  </style>
</head>
<body>
  {''.join(pages)}
</body>
</html>
"""

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html_doc).write_pdf(str(output_pdf))
    return flat_export


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
    parser = argparse.ArgumentParser(description="Generate NCEN family executive report.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "ncen_family_exec_report.pdf"),
        help="PDF output path",
    )
    args = parser.parse_args()

    project_id = _normalized(args.project_id)
    if not _is_effective_value(project_id):
        project_id = _detect_default_project_id()
    if not _is_effective_value(project_id):
        raise SystemExit("BigQuery project is empty. Set --project-id, BQ_PROJECT_ID, or GOOGLE_CLOUD_PROJECT.")

    rows = query_rows(project_id=project_id)
    export_rows = render_report(rows, Path(args.output))
    csv_path = Path(args.output).with_suffix(".csv")
    write_csv(export_rows, csv_path)

    print(f"Created report: {args.output}")
    print(f"Created flat data: {csv_path}")


if __name__ == "__main__":
    main()
