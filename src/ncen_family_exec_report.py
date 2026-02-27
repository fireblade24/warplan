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
    switch_reasoning: str
    likely_problems_ea_can_solve: str
    tier: str


# ... helpers

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


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _display(value: Any) -> str:
    return "" if value is None else str(value)


def _scoring_inputs(rows: list[dict[str, Any]]) -> tuple[int, int, float, float, int]:
    funds = len(rows)
    total_filings = sum(_to_int(r.get("total_filings_in_window")) for r in rows)
    qes_filings = sum(_to_int(r.get("qes_filings_in_window")) for r in rows)
    qes_share = (qes_filings / total_filings * 100.0) if total_filings else 0.0
    avg_agent_count = (
        sum(_to_int(r.get("total_agent_groups_used_in_window")) for r in rows) / funds if funds else 0.0
    )
    edgar_agents_llc_count = sum(
        1 for r in rows if str(r.get("ever_filed_by_edgar_agents_llc_in_window", "")).lower() == "true"
    )
    return funds, total_filings, qes_share, avg_agent_count, edgar_agents_llc_count


def _scores(rows: list[dict[str, Any]]) -> tuple[int, int]:
    funds, total_filings, qes_share, avg_agent_count, edgar_agents_llc_count = _scoring_inputs(rows)

    value_score = 0
    value_score += 3 if total_filings >= 1000 else 2 if total_filings >= 300 else 1 if total_filings >= 100 else 0
    value_score += 2 if funds >= 10 else 1 if funds >= 4 else 0

    switch_score = 0
    switch_score += 3 if qes_share < 25 else 2 if qes_share < 50 else 1 if qes_share < 70 else 0
    switch_score += 2 if avg_agent_count >= 4 else 1 if avg_agent_count >= 2 else 0
    switch_score += 1 if edgar_agents_llc_count > 0 else 0

    return value_score, switch_score


def summarize_family(rows: list[dict[str, Any]]) -> FamilyAiSummary:
    funds, total_filings, qes_share, avg_agent_count, edgar_agents_llc_count = _scoring_inputs(rows)
    value_score, switch_score = _scores(rows)

    if value_score >= 5:
        potential_value = "$$$$"
    elif value_score >= 3:
        potential_value = "$$$"
    elif value_score >= 2:
        potential_value = "$$"
    else:
        potential_value = "$"

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

    combined = value_score + switch_score
    if combined >= 8:
        tier = "Tier 1"
    elif combined >= 6:
        tier = "Tier 2"
    elif combined >= 4:
        tier = "Tier 3"
    else:
        tier = "Tier 4"

    switch_reasoning = (
        f"QES share is {qes_share:.2f}% across {funds} funds; average agent groups used is "
        f"{avg_agent_count:.2f}; {edgar_agents_llc_count} funds also used Edgar Agents LLC."
    )

    likely_problems = []
    if avg_agent_count >= 3:
        likely_problems.append("Fragmented vendor stack may create handoff delays and inconsistent workflows")
    if qes_share < 40:
        likely_problems.append("Low incumbent concentration suggests opportunity to consolidate accountability")
    if edgar_agents_llc_count > 0:
        likely_problems.append("Competitive vendor usage indicates split-book behavior and potential service gaps")
    if not likely_problems:
        likely_problems.append("Opportunity to improve cycle-time predictability and family-level reporting")

    conversation_script = (
        "We help fund families standardize filing operations across administrators and advisers. "
        "Could we review one recent filing cycle to pinpoint where we can reduce touches, "
        "improve turnaround consistency, and lower vendor-management overhead?"
    )

    return FamilyAiSummary(
        openness_to_switch=openness,
        potential_value_to_ea=potential_value,
        conversation_script=conversation_script,
        switch_reasoning=switch_reasoning,
        likely_problems_ea_can_solve="; ".join(likely_problems),
        tier=tier,
    )


def render_report(rows: list[dict[str, Any]], output_pdf: Path) -> list[dict[str, Any]]:
    families: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        fam = _display(row.get("ncen_family_investment_company_name")).strip()
        if fam:
            families.setdefault(fam, []).append(row)

    pages = []
    flat_export: list[dict[str, Any]] = []

    # Front summary page
    qes_form_counts: dict[str, int] = {}
    family_lines = []
    total_funds = len(rows)
    ea_funds = sum(1 for r in rows if str(r.get("ever_filed_by_edgar_agents_llc_in_window", "")).lower() == "true")

    for fam_name in sorted(families.keys()):
        fam_rows = families[fam_name]
        summary = summarize_family(fam_rows)
        funds_in_family = len(fam_rows)
        qes_funds_in_family = sum(1 for r in fam_rows if _to_int(r.get("qes_filings_in_window")) > 0)

        family_lines.append(
            f"<li><b>{html.escape(fam_name)}</b> — {summary.tier} | Value {html.escape(summary.potential_value_to_ea)} | "
            f"Switch {html.escape(summary.openness_to_switch)} | Total Funds: {funds_in_family} | "
            f"Funds QES Works With: {qes_funds_in_family}</li>"
        )

        for r in fam_rows:
            pairs = _display(r.get("qes_form_type_count_pairs"))
            if not pairs:
                continue
            for part in pairs.split("||"):
                if "::" not in part:
                    continue
                form_type, count_text = part.split("::", 1)
                try:
                    count_val = int(count_text)
                except Exception:
                    count_val = 0
                if form_type:
                    qes_form_counts[form_type] = qes_form_counts.get(form_type, 0) + count_val

    form_items = [
        f"<li><b>{html.escape(ft)}</b>: {cnt}</li>"
        for ft, cnt in sorted(qes_form_counts.items(), key=lambda x: (-x[1], x[0]))
    ]

    summary_page = f"""
<section class=\"family-page\">
  <h1>NCEN Executive Summary</h1>
  <div class=\"summary\">
    <p><b>Total Funds in Dataset:</b> {total_funds}</p>
    <p><b>Funds EA Has Also Filed For:</b> {ea_funds}</p>
  </div>
  <h2>All QES Form Types Across Dataset (with filing counts)</h2>
  <ul>{''.join(form_items) if form_items else '<li>No QES form types found.</li>'}</ul>
  <h2>Family Priority List (AI Tiering)</h2>
  <ul>{''.join(family_lines) if family_lines else '<li>No families found.</li>'}</ul>
</section>
"""
    pages.append(summary_page)

    for family_name in sorted(families.keys()):
        fund_rows = families[family_name]
        summary = summarize_family(fund_rows)

        fund_cards = []
        for r in fund_rows:
            fund_cards.append(
                f"""
<div class=\"fund-card\">
  <h3>{html.escape(_display(r.get('companyName')))} ({html.escape(_display(r.get('companyCik')))})</h3>
  <div class=\"grid\">
    <p><b>Investment Type:</b> {html.escape(_display(r.get('ncen_investment_company_type')))}</p>
    <p><b>Total Series:</b> {html.escape(_display(r.get('ncen_total_series')))}</p>
    <p><b>Accession Rows:</b> {html.escape(_display(r.get('ncen_accession_rows')))}</p>
    <p><b>Total Filings (Window):</b> {html.escape(_display(r.get('total_filings_in_window')))}</p>
    <p><b>QES Filings (Window):</b> {html.escape(_display(r.get('qes_filings_in_window')))}</p>
    <p><b>QES % of Filings:</b> {html.escape(_display(r.get('qes_pct_of_company_filings_in_window')))}%</p>
    <p><b>Filed by Edgar Agents LLC?</b> {html.escape(_display(r.get('ever_filed_by_edgar_agents_llc_in_window')))}</p>
    <p><b>Total Agent Groups:</b> {html.escape(_display(r.get('total_agent_groups_used_in_window')))}</p>
  </div>
  <p><b>QES Form Types for Fund:</b> {html.escape(_display(r.get('qes_form_types_for_fund')))}</p>
  <p><b>Admin Names:</b> {html.escape(_display(r.get('ncen_admin_names')))}</p>
  <p><b>Adviser Names:</b> {html.escape(_display(r.get('ncen_adviser_names')))}</p>
  <p><b>Adviser Types:</b> {html.escape(_display(r.get('ncen_adviser_types')))}</p>
  <p><b>Agent Groups Used:</b> {html.escape(_display(r.get('agent_groups_used_in_window')))}</p>
</div>
"""
            )

            export_row = dict(r)
            export_row["family_tier"] = summary.tier
            export_row["family_openness_to_switch"] = summary.openness_to_switch
            export_row["family_potential_value_to_ea"] = summary.potential_value_to_ea
            export_row["family_conversation_script"] = summary.conversation_script
            export_row["family_switch_reasoning"] = summary.switch_reasoning
            export_row["family_likely_problems_ea_can_solve"] = summary.likely_problems_ea_can_solve
            flat_export.append(export_row)

        page = f"""
<section class=\"family-page\">
  <h1>{html.escape(family_name)} ({html.escape(summary.tier)})</h1>
  <div class=\"summary\">
    <h2>AI Executive Summary</h2>
    <p><b>Openness to Switch:</b> {html.escape(summary.openness_to_switch)}</p>
    <p><b>Potential Value to EA:</b> {html.escape(summary.potential_value_to_ea)}</p>
    <p><b>Switch Likelihood Reasoning:</b> {html.escape(summary.switch_reasoning)}</p>
    <p><b>Likely Problems EA Can Solve:</b> {html.escape(summary.likely_problems_ea_can_solve)}</p>
    <p><b>Conversation Starter Script:</b> {html.escape(summary.conversation_script)}</p>
  </div>
  <h2>Funds in Family</h2>
  {''.join(fund_cards)}
</section>
"""
        pages.append(page)

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <style>
    @page {{ size: Letter portrait; margin: 0.35in; }}
    body {{ font-family: Arial, sans-serif; font-size: 10px; color: #1c1c1c; }}
    h1 {{ font-size: 22px; margin: 0 0 8px 0; border-bottom: 2px solid #0c4a6e; padding-bottom: 5px; }}
    h2 {{ font-size: 14px; margin: 8px 0 6px 0; color: #334155; }}
    h3 {{ font-size: 12px; margin: 0 0 6px 0; color: #0f172a; }}
    ul {{ margin: 4px 0 10px 18px; }}
    li {{ margin: 2px 0; }}
    .family-page {{ page-break-after: always; }}
    .family-page:last-child {{ page-break-after: auto; }}
    .summary {{ background: #f8fafc; border: 1px solid #cbd5e1; padding: 10px; margin-bottom: 10px; }}
    .fund-card {{ border: 1px solid #d1d5db; border-left: 4px solid #0c4a6e; padding: 8px; margin-bottom: 8px; background: #fff; }}
    .fund-card p {{ margin: 2px 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 4px 10px; }}
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
