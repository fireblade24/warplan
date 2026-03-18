#!/usr/bin/env python3
"""Generate the Action Plan QES/FilePoint report using selected sections from the NCEN multi-agent report."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from ncen_multi_agent_fund_family_report import (
    OUTPUT_DIR,
    _build_section_11_outputs,
    _build_section_6_rows,
    _chunked,
    _detect_default_project_id,
    _format_three_column_list,
    _is_effective_value,
    _normalized,
    _prepare_rows,
    _table,
    query_rows,
    write_csv,
)


ROOT = Path(__file__).resolve().parents[1]


def _render_action_section_pages(section_num: str, title: str, subtitle: str, headers: list[str], rows: list[list[str]], rows_per_page: int = 25) -> str:
    chunks = _chunked(rows, rows_per_page)
    total_pages = len(chunks)
    pages = []
    for idx, chunk in enumerate(chunks, start=1):
        pages.append(
            "\n".join(
                [
                    '<section class="page">',
                    f"<h1>Section {section_num}: {title}</h1>",
                    f"<p>{subtitle}</p>",
                    _table(headers, chunk),
                    f'<div class="page-number">Page {idx} of {total_pages}</div>',
                    "</section>",
                ]
            )
        )
    return "\n".join(pages)


def render_report(rows: list[dict[str, Any]], output_pdf: Path) -> dict[str, list[dict[str, Any]]]:
    from weasyprint import HTML

    prepared = _prepare_rows(rows)
    fund_rows = prepared["fund_rows"]

    section_1_rows = _format_three_column_list(prepared["families_qes"])
    section_2_rows = _format_three_column_list(prepared["families_fp"])
    section_3_rows = _format_three_column_list(prepared["families_qes_ea"])
    section_4_rows = _format_three_column_list(prepared["families_fp_ea"])
    section_5_rows = _format_three_column_list(prepared["families_qes_ea_fp"])
    section_6_rows = _build_section_6_rows(fund_rows, set(prepared["families_qes_ea_fp"]))
    section_7_rows = _build_section_6_rows(fund_rows, set(prepared["families_qes_ea"]))
    section_8_rows = _build_section_6_rows(fund_rows, set(prepared["families_fp_ea"]))

    section_11 = _build_section_11_outputs(fund_rows)
    section_9_rows = [
        [
            r["Administrator"],
            str(r["Total Filings"]),
            str(r["EA Count"]),
            str(r["QES Count"]),
            str(r["FilePoint Count"]),
            str(r["Other Count"]),
            f"{r['EA %']}%",
            f"{r['QES %']}%",
            f"{r['FilePoint %']}%",
            f"{r['Other %']}%",
        ]
        for r in section_11["summary_admin"]
    ]
    section_10_rows = [
        [
            r["Administrator"],
            r["Fund Family"],
            r["EA Presence (Yes/No)"],
            r["Competitors Present (QES/FilePoint/Both)"],
            r["Fund Family Agent Mix"],
            r["Opportunity Type (Expansion / New / Defend)"],
            str(r["Number of Funds"]),
            str(r["Number of High-Value Filings"]),
        ]
        for r in section_11["opportunity"]
    ]

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <style>
    @page {{ size: Letter landscape; margin: 0.35in; }}
    body {{ font-family: Arial, sans-serif; font-size: 8.5px; line-height: 1.15; color: #111827; }}
    .page {{ page-break-after: always; position: relative; min-height: 7.15in; }}
    .page:last-child {{ page-break-after: auto; }}
    h1 {{ font-size: 16px; margin: 0 0 4px 0; border-bottom: 2px solid #0c4a6e; padding-bottom: 3px; }}
    p {{ margin: 2px 0 6px 0; color: #334155; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; margin-bottom: 8px; page-break-inside: auto; }}
    thead {{ display: table-header-group; }}
    tbody {{ display: table-row-group; }}
    tr {{ page-break-inside: avoid; break-inside: avoid; }}
    th, td {{ border: 1px solid #d1d5db; padding: 3px; text-align: left; vertical-align: top; word-wrap: break-word; overflow-wrap: anywhere; }}
    th {{ background: #f1f5f9; }}
    .page-number {{ position: absolute; bottom: 0; right: 0; font-size: 9px; color: #475569; }}
  </style>
</head>
<body>
  {_render_action_section_pages('1', 'All Fund Families where QES is a client filing agent', 'Unique fund families with QES presence.', ['Fund Family', 'Fund Family', 'Fund Family'], section_1_rows, rows_per_page=38)}
  {_render_action_section_pages('2', 'All Fund Families where File Point appears', 'Unique fund families with File Point presence.', ['Fund Family', 'Fund Family', 'Fund Family'], section_2_rows, rows_per_page=38)}
  {_render_action_section_pages('3', 'Fund Families in Common: QES and EA', 'Families where both QES and EA file at least one fund.', ['Fund Family', 'Fund Family', 'Fund Family'], section_3_rows, rows_per_page=38)}
  {_render_action_section_pages('4', 'Fund Families in Common: File Point and EA', 'Families where both File Point and EA file at least one fund.', ['Fund Family', 'Fund Family', 'Fund Family'], section_4_rows, rows_per_page=38)}
  {_render_action_section_pages('5', 'Fund Families in Common: QES, EA, and File Point', 'Families where all three filing agents are present.', ['Fund Family', 'Fund Family', 'Fund Family'], section_5_rows, rows_per_page=38)}
  {_render_action_section_pages('6', 'QES + EA + File Point Common Families with Forms by Fund', 'Shows admin, forms each agent files, and whether each agent files each fund.', ['Fund Family', 'Fund', 'Admin(s)', 'QES Files?', 'QES Forms', 'EA Files?', 'EA Forms', 'File Point Files?', 'File Point Forms'], section_6_rows, rows_per_page=13)}
  {_render_action_section_pages('7', 'QES + EA Families with Forms by Fund', 'Shows admin, forms each agent files, and whether each agent files each fund.', ['Fund Family', 'Fund', 'Admin(s)', 'QES Files?', 'QES Forms', 'EA Files?', 'EA Forms', 'File Point Files?', 'File Point Forms'], section_7_rows, rows_per_page=13)}
  {_render_action_section_pages('8', 'File Point + EA Families with Forms by Fund', 'Shows admin, forms each agent files, and whether each agent files each fund.', ['Fund Family', 'Fund', 'Admin(s)', 'QES Files?', 'QES Forms', 'EA Files?', 'EA Forms', 'File Point Files?', 'File Point Forms'], section_8_rows, rows_per_page=13)}
  {_render_action_section_pages('9', 'Summary Table by Admin (Filing Agent Distribution)', 'Filing distribution and share by admin across EA, QES, FilePoint, and Other.', ['Administrator', 'Total Filings', 'EA Count', 'QES Count', 'FilePoint Count', 'Other Count', 'EA %', 'QES %', 'FilePoint %', 'Other %'], section_9_rows, rows_per_page=20)}
  {_render_action_section_pages('10', 'Opportunity Table (EA Expansion / New / Defend)', 'Family-level opportunity flags, agent mix, and high-value filing indicators.', ['Administrator', 'Fund Family', 'EA Presence', 'Competitors Present', 'Agent Mix', 'Opportunity Type', '# Funds', '# High-Value Filings'], section_10_rows, rows_per_page=18)}
</body>
</html>
"""

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html_doc).write_pdf(str(output_pdf))

    summary_rows = [
        {"section": "section_1_qes_families", "row_count": len(section_1_rows)},
        {"section": "section_2_file_point_families", "row_count": len(section_2_rows)},
        {"section": "section_3_qes_ea_families", "row_count": len(section_3_rows)},
        {"section": "section_4_file_point_ea_families", "row_count": len(section_4_rows)},
        {"section": "section_5_qes_ea_file_point_families", "row_count": len(section_5_rows)},
        {"section": "section_6_qes_ea_file_point_fund_forms", "row_count": len(section_6_rows)},
        {"section": "section_7_qes_ea_fund_forms", "row_count": len(section_7_rows)},
        {"section": "section_8_file_point_ea_fund_forms", "row_count": len(section_8_rows)},
        {"section": "section_9_admin_distribution", "row_count": len(section_9_rows)},
        {"section": "section_10_opportunity", "row_count": len(section_10_rows)},
    ]
    return {
        "summary": summary_rows,
        "summary_admin": section_11["summary_admin"],
        "opportunity": section_11["opportunity"],
        "pivot_admin_agent": section_11["pivot_admin_agent"],
        "pivot_admin_form_agent": section_11["pivot_admin_form_agent"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the Action Plan QES/FilePoint report.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "action_plan_qes_filepoint.pdf"),
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
    write_csv(sections["summary"], output_base.with_suffix(".csv"))
    write_csv(sections["summary_admin"], output_base.with_name(output_base.name + "_summary_by_admin.csv"))
    write_csv(sections["opportunity"], output_base.with_name(output_base.name + "_opportunity.csv"))
    write_csv(sections["pivot_admin_agent"], output_base.with_name(output_base.name + "_pivot_admin_agent.csv"))
    write_csv(sections["pivot_admin_form_agent"], output_base.with_name(output_base.name + "_pivot_admin_form_agent.csv"))


if __name__ == "__main__":
    main()
