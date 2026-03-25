#!/usr/bin/env python3
"""Generate PDF/CSV for the 10 newest filings from NCEN-enriched source view."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import subprocess
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "ncen_newest_filings_report.sql"
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


def _display(value: Any) -> str:
    return "" if value is None else str(value)


def render_pdf(rows: list[dict[str, Any]], output_pdf: Path) -> None:
    from weasyprint import HTML

    cards = []
    for i, r in enumerate(rows, start=1):
        cards.append(
            f"""
<div class=\"card\">
  <h2>#{i} {_display(r.get('companyName'))} ({_display(r.get('companyCik'))})</h2>
  <div class=\"grid\">
    <p><b>Filing Date:</b> {html.escape(_display(r.get('filingDate')))}</p>
    <p><b>Index Date:</b> {html.escape(_display(r.get('indexDate')))}</p>
    <p><b>Load Timestamp:</b> {html.escape(_display(r.get('load_ts')))}</p>
    <p><b>Form Type:</b> {html.escape(_display(r.get('formType')))}</p>
    <p><b>Accession #:</b> {html.escape(_display(r.get('accessionNumber')))}</p>
    <p><b>Filing Agent Group:</b> {html.escape(_display(r.get('filing_agent_group')))}</p>
    <p><b>Agent Category:</b> {html.escape(_display(r.get('agent_category')))}</p>
    <p><b>Agent Category Refined:</b> {html.escape(_display(r.get('agent_category_refined')))}</p>
    <p><b>Is Filing Agent:</b> {html.escape(_display(r.get('is_filing_agent')))}</p>
    <p><b>Is Self Filer:</b> {html.escape(_display(r.get('is_self_filer')))}</p>
    <p><b>NCEN Registrant Name:</b> {html.escape(_display(r.get('ncen_registrant_name')))}</p>
    <p><b>NCEN File #:</b> {html.escape(_display(r.get('ncen_file_num')))}</p>
    <p><b>Family:</b> {html.escape(_display(r.get('ncen_family_investment_company_name')))}</p>
    <p><b>Investment Type:</b> {html.escape(_display(r.get('ncen_investment_company_type')))}</p>
    <p><b>Total Series:</b> {html.escape(_display(r.get('ncen_total_series')))}</p>
    <p><b>Accession Rows:</b> {html.escape(_display(r.get('ncen_accession_rows')))}</p>
  </div>
  <p><b>Admin Names:</b> {html.escape(_display(r.get('ncen_admin_names')))}</p>
  <p><b>Adviser Names:</b> {html.escape(_display(r.get('ncen_adviser_names')))}</p>
  <p><b>Adviser Types:</b> {html.escape(_display(r.get('ncen_adviser_types')))}</p>
</div>
"""
        )

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <style>
    @page {{ size: Letter portrait; margin: 0.35in; }}
    body {{ font-family: Arial, sans-serif; font-size: 10px; color: #1c1c1c; }}
    h1 {{ font-size: 20px; margin: 0 0 8px 0; border-bottom: 2px solid #0c4a6e; padding-bottom: 4px; }}
    h2 {{ font-size: 13px; margin: 0 0 6px 0; color: #0f172a; }}
    .card {{ border: 1px solid #d1d5db; border-left: 4px solid #0c4a6e; padding: 8px; margin-bottom: 8px; page-break-inside: avoid; }}
    .card p {{ margin: 2px 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 4px 10px; }}
  </style>
</head>
<body>
  <h1>10 Newest Filings — NCEN Enriched</h1>
  {''.join(cards) if cards else '<p>No rows returned.</p>'}
</body>
</html>
"""

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html_doc).write_pdf(str(output_pdf))


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
    parser = argparse.ArgumentParser(description="Generate PDF/CSV for 10 newest filings.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "ncen_newest_filings_report.pdf"),
        help="PDF output path",
    )
    args = parser.parse_args()

    project_id = _normalized(args.project_id)
    if not _is_effective_value(project_id):
        project_id = _detect_default_project_id()
    if not _is_effective_value(project_id):
        raise SystemExit("BigQuery project is empty. Set --project-id, BQ_PROJECT_ID, or GOOGLE_CLOUD_PROJECT.")

    rows = query_rows(project_id)
    render_pdf(rows, Path(args.output))
    csv_path = Path(args.output).with_suffix(".csv")
    write_csv(rows, csv_path)

    print(f"Created report: {args.output}")
    print(f"Created csv: {csv_path}")


if __name__ == "__main__":
    main()
