#!/usr/bin/env python3
"""Generate a QUALITY EDGAR SOLUTIONS vendor intelligence report.

The script:
1) pulls company-level metrics from BigQuery (`fact_filing_enriched`),
2) enriches each company with AI-style rankings, and
3) exports a PDF report via WeasyPrint.
"""

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

QES_NAME = "QUALITY EDGAR SOLUTIONS"
ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "quality_edgar_vendor_report.sql"
OUTPUT_DIR = ROOT / "output"


@dataclass
class AiAssessment:
    money_rank: str
    switch_rank: str
    reasoning: str


def load_sql(project_id: str, dataset_id: str) -> str:
    sql = SQL_PATH.read_text(encoding="utf-8")
    return sql.replace("@project_id", project_id).replace("@dataset_id", dataset_id)


def query_company_metrics(project_id: str, dataset_id: str) -> list[dict[str, Any]]:
    sql = load_sql(project_id=project_id, dataset_id=dataset_id)
    cmd = [
        "bq",
        "query",
        "--project_id",
        project_id,
        "--use_legacy_sql=false",
        "--format=prettyjson",
        "--",
        sql,
    ]

    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"bq query failed: {result.stderr.strip()}")

    payload = result.stdout.strip()
    if not payload:
        return []

    data = json.loads(payload)
    if isinstance(data, list):
        return [dict(row) for row in data]

    raise RuntimeError("Unexpected bq output format. Expected JSON array.")


def score_company(row: dict[str, Any]) -> AiAssessment:
    total = int(row["total_filings"])
    qes_pct = float(row["qes_percentage"])
    other_agents = int(row["other_agents_count"])
    dominant = bool(row["is_qes_dominant_filer"])
    last_form = str(row.get("qes_last_form_type", "") or "")

    high_value_forms = {"S-1", "S-3", "10-K", "10-Q", "8-K", "DEF 14A", "424B"}
    complex_form_bonus = 1 if any(tag in last_form.upper() for tag in high_value_forms) else 0

    revenue_score = 0
    revenue_score += 3 if total >= 80 else 2 if total >= 40 else 1 if total >= 15 else 0
    revenue_score += 2 if qes_pct >= 70 else 1 if qes_pct >= 35 else 0
    revenue_score += complex_form_bonus

    if revenue_score >= 6:
        money_rank = "$$$$"
    elif revenue_score >= 4:
        money_rank = "$$$"
    elif revenue_score >= 2:
        money_rank = "$$"
    else:
        money_rank = "$"

    switch_score = 0
    switch_score += 3 if qes_pct < 20 else 2 if qes_pct < 40 else 1 if qes_pct < 55 else 0
    switch_score += 2 if other_agents >= 3 else 1 if other_agents >= 1 else 0
    switch_score += 1 if not dominant else 0

    if switch_score >= 6:
        switch_rank = "Very Likely"
    elif switch_score >= 4:
        switch_rank = "Likely"
    elif switch_score >= 3:
        switch_rank = "Possible"
    elif switch_score >= 2:
        switch_rank = "Low"
    else:
        switch_rank = "Very Low"

    reasoning = (
        f"Total filings={total}, {QES_NAME} share={qes_pct:.2f}%, "
        f"other agents={other_agents}, dominant={dominant}."
    )

    return AiAssessment(money_rank=money_rank, switch_rank=switch_rank, reasoning=reasoning)


def apply_ai_assessments(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        scored = dict(row)
        assessment = score_company(scored)
        scored["money_rank"] = assessment.money_rank
        scored["switch_rank"] = assessment.switch_rank
        scored["ai_reasoning"] = assessment.reasoning
        enriched_rows.append(scored)
    return enriched_rows


def _display_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def render_pdf(rows: list[dict[str, Any]], output_path: Path) -> None:
    table_rows = []
    for row in rows:
        company_name = html.escape(_display_value(row.get("companyName")))
        company_cik = html.escape(_display_value(row.get("companyCIK")))
        total_filings = html.escape(_display_value(row.get("total_filings")))
        qes_filings = html.escape(_display_value(row.get("qes_filings")))
        qes_percentage = html.escape(_display_value(row.get("qes_percentage")))
        dominant_filer = "Yes" if row.get("is_qes_dominant_filer") else "No"
        other_agents_count = html.escape(_display_value(row.get("other_agents_count")))
        qes_vendor_since = html.escape(_display_value(row.get("qes_vendor_since")))
        qes_last_filing_date = html.escape(_display_value(row.get("qes_last_filing_date")))
        qes_last_form_type = html.escape(_display_value(row.get("qes_last_form_type")))
        money_rank = html.escape(_display_value(row.get("money_rank")))
        switch_rank = html.escape(_display_value(row.get("switch_rank")))
        ai_reasoning = html.escape(_display_value(row.get("ai_reasoning")))

        table_rows.append(
            "<tr>"
            f"<td>{company_name}</td>"
            f"<td>{company_cik}</td>"
            f"<td>{total_filings}</td>"
            f"<td>{qes_filings}</td>"
            f"<td>{qes_percentage}%</td>"
            f"<td>{dominant_filer}</td>"
            f"<td>{other_agents_count}</td>"
            f"<td>{qes_vendor_since}</td>"
            f"<td>{qes_last_filing_date}</td>"
            f"<td>{qes_last_form_type}</td>"
            f"<td>{money_rank}</td>"
            f"<td>{switch_rank}</td>"
            f"<td>{ai_reasoning}</td>"
            "</tr>"
        )

    html_string = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    body {{ font-family: Arial, sans-serif; font-size: 11px; color: #111; }}
    h1 {{ font-size: 20px; margin-bottom: 4px; }}
    h2 {{ font-size: 13px; margin-top: 0; color: #444; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
    th, td {{
      border: 1px solid #d7d7d7;
      padding: 6px;
      vertical-align: top;
      word-wrap: break-word;
    }}
    th {{ background: #f4f4f4; }}
    .small {{ color: #666; font-size: 10px; }}
  </style>
</head>
<body>
  <h1>{html.escape(QES_NAME)} Client Opportunity Report</h1>
  <h2>One row per company using {html.escape(QES_NAME)}</h2>
  <p class="small">Generated automatically from BigQuery + AI scoring.</p>
  <table>
    <thead>
      <tr>
        <th>Company</th>
        <th>CIK</th>
        <th>Total Filings</th>
        <th>{html.escape(QES_NAME)} Filings</th>
        <th>{html.escape(QES_NAME)} %</th>
        <th>Dominant Filer?</th>
        <th>Other Agents</th>
        <th>Vendor Since</th>
        <th>Last Filing Date</th>
        <th>Last Form</th>
        <th>Revenue Rank</th>
        <th>Switch Likelihood</th>
        <th>AI Notes</th>
      </tr>
    </thead>
    <tbody>
      {''.join(table_rows)}
    </tbody>
  </table>
</body>
</html>
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html_string).write_pdf(str(output_path))


def write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _display_value(v) for k, v in row.items()})


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate QES vendor report PDF.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument("--dataset-id", default=os.getenv("BQ_DATASET_ID"), required=False)
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "quality_edgar_vendor_report.pdf"),
        help="PDF output path",
    )
    args = parser.parse_args()

    if not args.project_id or not args.dataset_id:
        raise SystemExit("Both --project-id and --dataset-id (or env vars) are required.")

    metrics_rows = query_company_metrics(project_id=args.project_id, dataset_id=args.dataset_id)
    report_rows = apply_ai_assessments(metrics_rows)
    render_pdf(report_rows, Path(args.output))
    csv_path = Path(args.output).with_suffix(".csv")
    write_csv(report_rows, csv_path)

    print(f"Created report: {args.output}")
    print(f"Created flat data: {csv_path}")


if __name__ == "__main__":
    main()
