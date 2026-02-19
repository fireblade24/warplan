#!/usr/bin/env python3
"""Generate a QUALITY EDGAR SOLUTIONS vendor intelligence report.

The script:
1) pulls company-level metrics from BigQuery (`fact_filing_enriched`),
2) enriches each company with AI-style rankings, and
3) exports a PDF report via WeasyPrint.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from jinja2 import Template
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


def query_company_metrics(project_id: str, dataset_id: str) -> pd.DataFrame:
    client = bigquery.Client(project=project_id)
    sql = load_sql(project_id=project_id, dataset_id=dataset_id)
    return client.query(sql).to_dataframe(create_bqstorage_client=False)


def score_company(row: pd.Series) -> AiAssessment:
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


def apply_ai_assessments(df: pd.DataFrame) -> pd.DataFrame:
    assessments = df.apply(score_company, axis=1)
    df = df.copy()
    df["money_rank"] = [a.money_rank for a in assessments]
    df["switch_rank"] = [a.switch_rank for a in assessments]
    df["ai_reasoning"] = [a.reasoning for a in assessments]
    return df


def render_pdf(df: pd.DataFrame, output_path: Path) -> None:
    template = Template(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    body { font-family: Arial, sans-serif; font-size: 11px; color: #111; }
    h1 { font-size: 20px; margin-bottom: 4px; }
    h2 { font-size: 13px; margin-top: 0; color: #444; }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; }
    th, td {
      border: 1px solid #d7d7d7;
      padding: 6px;
      vertical-align: top;
      word-wrap: break-word;
    }
    th { background: #f4f4f4; }
    .small { color: #666; font-size: 10px; }
  </style>
</head>
<body>
  <h1>{{ vendor }} Client Opportunity Report</h1>
  <h2>One row per company using {{ vendor }}</h2>
  <p class="small">Generated automatically from BigQuery + AI scoring.</p>
  <table>
    <thead>
      <tr>
        <th>Company</th>
        <th>CIK</th>
        <th>Total Filings</th>
        <th>{{ vendor }} Filings</th>
        <th>{{ vendor }} %</th>
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
    {% for row in rows %}
      <tr>
        <td>{{ row.companyName }}</td>
        <td>{{ row.companyCIK }}</td>
        <td>{{ row.total_filings }}</td>
        <td>{{ row.qes_filings }}</td>
        <td>{{ row.qes_percentage }}%</td>
        <td>{{ "Yes" if row.is_qes_dominant_filer else "No" }}</td>
        <td>{{ row.other_agents_count }}</td>
        <td>{{ row.qes_vendor_since }}</td>
        <td>{{ row.qes_last_filing_date }}</td>
        <td>{{ row.qes_last_form_type }}</td>
        <td>{{ row.money_rank }}</td>
        <td>{{ row.switch_rank }}</td>
        <td>{{ row.ai_reasoning }}</td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
</body>
</html>
        """
    )

    rows = df.fillna("").to_dict(orient="records")
    html = template.render(vendor=QES_NAME, rows=rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html).write_pdf(str(output_path))


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

    metrics_df = query_company_metrics(project_id=args.project_id, dataset_id=args.dataset_id)
    report_df = apply_ai_assessments(metrics_df)
    render_pdf(report_df, Path(args.output))
    csv_path = Path(args.output).with_suffix(".csv")
    report_df.to_csv(csv_path, index=False)

    print(f"Created report: {args.output}")
    print(f"Created flat data: {csv_path}")


if __name__ == "__main__":
    main()
