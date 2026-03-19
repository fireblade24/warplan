#!/usr/bin/env python3
"""Generate the Action Plan QES/FilePoint report using selected sections from the NCEN multi-agent report."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
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


def _normalize_name(value: str) -> str:
    text = (value or "").upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\b(FUND|FUNDS|TRUST|PORTFOLIO|PORTFOLIOS|SERIES|INC|LLC|LTD|PLC|CORP|CORPORATION|COMPANY|CO)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def query_sales_rows(project_id: str) -> list[dict[str, Any]]:
    sql = """
    SELECT
      string_field_0 AS fund_family_name,
      string_field_1 AS sales_person
    FROM `sec-edgar-ralph.warplan.client_list`
    WHERE TRIM(COALESCE(string_field_0, '')) <> ''
      AND TRIM(COALESCE(string_field_1, '')) <> ''
    """
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


def _build_sales_map(sales_rows: list[dict[str, Any]]) -> dict[str, set[str]]:
    sales_map: dict[str, set[str]] = {}
    for row in sales_rows:
        family = str(row.get("fund_family_name") or "").strip()
        sales_person = str(row.get("sales_person") or "").strip()
        if not family or not sales_person:
            continue
        sales_map.setdefault(_normalize_name(family), set()).add(sales_person)
    return sales_map


def _lookup_sales_matches(family_name: str, fund_name: str, sales_map: dict[str, set[str]]) -> tuple[list[str], str]:
    normalized_family = _normalize_name(family_name)
    normalized_fund = _normalize_name(fund_name)
    family_matches = sorted(sales_map.get(normalized_family, set()))
    fund_matches = sorted(sales_map.get(normalized_fund, set()))
    if family_matches:
        return family_matches, "Fund Family"
    if fund_matches:
        return fund_matches, "Fund Name"
    return [], ""


def _build_sales_relationship_outputs(fund_rows: list[dict[str, Any]], sales_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    sales_map = _build_sales_map(sales_rows)

    relationship_rows: list[dict[str, Any]] = []
    action_rows: list[dict[str, Any]] = []

    for fr in sorted(fund_rows, key=lambda x: (x["family"], x["fund"])):
        sales_people, match_source = _lookup_sales_matches(fr["family"], fr["fund"], sales_map)
        if not sales_people:
            continue

        competitor_agents = []
        if fr["has_qes"]:
            competitor_agents.append("QES")
        if fr["has_fp"]:
            competitor_agents.append("FilePoint")
        competitors = ", ".join(competitor_agents) if competitor_agents else "None"

        ea_forms = {f.strip() for f in fr["ea_forms"].split(",") if f.strip()}
        non_ea_forms = sorted(
            ({f.strip() for f in fr["qes_forms"].split(",") if f.strip()} | {f.strip() for f in fr["fp_forms"].split(",") if f.strip()}) - ea_forms
        )
        available_form_list = ", ".join(non_ea_forms) or "-"
        relationship_form_types = sorted(
            {
                *[f.strip() for f in fr["qes_forms"].split(",") if f.strip()],
                *[f.strip() for f in fr["ea_forms"].split(",") if f.strip()],
                *[f.strip() for f in fr["fp_forms"].split(",") if f.strip()],
            }
        )
        relationship_form_list = ", ".join(relationship_form_types) or "-"

        if fr["has_ea"] and competitors != "None":
            opportunity = "Expansion"
            reason = f"EA already has a relationship here, but {competitors} also file this relationship."
        elif fr["has_ea"]:
            opportunity = "Defend"
            reason = "EA is the only visible filing agent in this tracked universe and should be defended."
        else:
            opportunity = "New"
            reason = f"EA is not present but {competitors} file this relationship today."

        for sales_person in sales_people:
            relationship_rows.append(
                {
                    "Sales Person": sales_person,
                    "Match Source": match_source,
                    "Administrator": "; ".join(fr["admins"]) or "-",
                    "Fund Family": fr["family"],
                    "Fund": fr["fund"],
                    "EA Present": "Yes" if fr["has_ea"] else "No",
                    "QES Present": "Yes" if fr["has_qes"] else "No",
                    "FilePoint Present": "Yes" if fr["has_fp"] else "No",
                    "Opportunity": opportunity,
                    "Form Types": relationship_form_list,
                }
            )
            action_rows.append(
                {
                    "Sales Person": sales_person,
                    "Action Group": opportunity,
                    "Match Source": match_source,
                    "Administrator": "; ".join(fr["admins"]) or "-",
                    "Fund Family": fr["family"],
                    "Fund": fr["fund"],
                    "Reason": reason,
                    "Form Types Available": available_form_list,
                }
            )

    action_priority = {"Expansion": 0, "Defend": 1, "New": 2}
    relationship_rows.sort(key=lambda x: (x["Sales Person"], x["Administrator"], x["Fund Family"], x["Fund"]))
    action_rows.sort(key=lambda x: (x["Sales Person"], action_priority.get(x["Action Group"], 99), x["Administrator"], x["Fund Family"], x["Fund"]))
    return {"relationship": relationship_rows, "actions": action_rows}


def _build_sales_new_opportunity_outputs(fund_rows: list[dict[str, Any]], sales_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sales_map = _build_sales_map(sales_rows)
    existing_relationships: dict[tuple[str, str], set[str]] = {}
    output_rows: list[dict[str, Any]] = []
    seen_rows: set[tuple[str, str, str, str, str]] = set()

    for fr in fund_rows:
        sales_people, _match_source = _lookup_sales_matches(fr["family"], fr["fund"], sales_map)
        if not sales_people or not fr["has_ea"]:
            continue
        current_item = f"{fr['family']} :: {fr['fund']}"
        for admin in fr["admins"]:
            for sales_person in sales_people:
                existing_relationships.setdefault((sales_person, admin), set()).add(current_item)

    for fr in sorted(fund_rows, key=lambda x: (x["family"], x["fund"])):
        if fr["has_ea"] or not (fr["has_qes"] or fr["has_fp"]):
            continue

        competitor_agents = []
        if fr["has_qes"]:
            competitor_agents.append("QES")
        if fr["has_fp"]:
            competitor_agents.append("FilePoint")
        competitors = ", ".join(competitor_agents) if competitor_agents else "None"
        form_types_available = sorted(
            {
                *[f.strip() for f in fr["qes_forms"].split(",") if f.strip()],
                *[f.strip() for f in fr["fp_forms"].split(",") if f.strip()],
            }
        )
        if {"NPORT-P", "NPORT-P/A"} & set(form_types_available):
            continue
        form_type_available_list = ", ".join(form_types_available) or "-"

        for admin in fr["admins"]:
            for sales_person, _admin in sorted(existing_relationships):
                if _admin != admin:
                    continue
                existing_items = sorted(existing_relationships[(sales_person, admin)])
                if not existing_items:
                    continue
                row_key = (sales_person, admin, fr["family"], fr["fund"], "|".join(existing_items))
                if row_key in seen_rows:
                    continue
                seen_rows.add(row_key)
                output_rows.append(
                    {
                        "Sales Person": sales_person,
                        "Administrator": admin,
                        "Current EA Relationship": "; ".join(existing_items),
                        "Related Opportunity Family": fr["family"],
                        "Related Opportunity Fund": fr["fund"],
                        "Competing Filer(s)": competitors,
                        "Form Types Available": form_type_available_list,
                    }
                )

    output_rows.sort(
        key=lambda x: (
            x["Sales Person"],
            x["Administrator"],
            x["Related Opportunity Family"],
            x["Related Opportunity Fund"],
        )
    )
    return output_rows


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


def render_report(rows: list[dict[str, Any]], sales_rows: list[dict[str, Any]], output_pdf: Path) -> dict[str, list[dict[str, Any]]]:
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
    section_sales = _build_sales_relationship_outputs(fund_rows, sales_rows)
    section_sales_new_opportunity = _build_sales_new_opportunity_outputs(fund_rows, sales_rows)
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

    section_11_relationship_rows = [
        [
            r["Sales Person"],
            r["Match Source"],
            r["Administrator"],
            r["Fund Family"],
            r["Fund"],
            r["EA Present"],
            r["QES Present"],
            r["FilePoint Present"],
            r["Opportunity"],
            r["Form Types"],
        ]
        for r in section_sales["relationship"]
    ]
    section_11_action_rows = [
        [
            r["Sales Person"],
            r["Action Group"],
            r["Match Source"],
            r["Administrator"],
            r["Fund Family"],
            r["Fund"],
            r["Reason"],
            r["Form Types Available"],
        ]
        for r in section_sales["actions"]
    ]
    section_11_new_opportunity_rows = [
        [
            r["Sales Person"],
            r["Administrator"],
            r["Current EA Relationship"],
            r["Related Opportunity Family"],
            r["Related Opportunity Fund"],
            r["Competing Filer(s)"],
            r["Form Types Available"],
        ]
        for r in section_sales_new_opportunity
    ]
    sales_people_for_actions = sorted({r["Sales Person"] for r in section_sales["actions"]})
    section_11_action_pages = []
    for sales_person in sales_people_for_actions:
        salesperson_rows = [
            [
                r["Sales Person"],
                r["Action Group"],
                r["Match Source"],
                r["Administrator"],
                r["Fund Family"],
                r["Fund"],
                r["Reason"],
                r["Form Types Available"],
            ]
            for r in section_sales["actions"]
            if r["Sales Person"] == sales_person
        ]
        section_11_action_pages.append(
            _render_action_section_pages(
                "11.2",
                f"Sales Person Action List — {sales_person}",
                "Action list for this sales person grouped into Expansion, Defend, and New. Form Types Available show the forms EA does not file yet, and reasons name the competing filer when present.",
                ["Sales Person", "Action Group", "Match Source", "Administrator", "Fund Family", "Fund", "Reason", "Form Types Available"],
                salesperson_rows,
                rows_per_page=15,
            )
        )
    sales_people_for_new_opportunity = sorted({r["Sales Person"] for r in section_sales_new_opportunity})
    section_11_new_opportunity_pages = []
    for sales_person in sales_people_for_new_opportunity:
        salesperson_rows = [
            [
                r["Sales Person"],
                r["Administrator"],
                r["Current EA Relationship"],
                r["Related Opportunity Family"],
                r["Related Opportunity Fund"],
                r["Competing Filer(s)"],
                r["Form Types Available"],
            ]
            for r in section_sales_new_opportunity
            if r["Sales Person"] == sales_person
        ]
        section_11_new_opportunity_pages.append(
            _render_action_section_pages(
                "11.3",
                f"Same-Admin New Opportunity Assignment — {sales_person}",
                "New-opportunity assignments for this sales person. Funds with NPORT-P or NPORT-P/A are excluded because they are not EA opportunity targets.",
                ["Sales Person", "Administrator", "Current EA Relationship", "Related Opportunity Family", "Related Opportunity Fund", "Competing Filer(s)", "Form Types Available"],
                salesperson_rows,
                rows_per_page=15,
            )
        )

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
  {_render_action_section_pages('11.1', 'Sales Person Relationship', 'Shows all sales-person matches using fund family first and fund name as fallback, plus admin/fund/agent opportunity context.', ['Sales Person', 'Match Source', 'Administrator', 'Fund Family', 'Fund', 'EA', 'QES', 'FilePoint', 'Opportunity', 'Form Types'], section_11_relationship_rows, rows_per_page=15)}
  {"".join(section_11_action_pages)}
  {"".join(section_11_new_opportunity_pages)}
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
        {"section": "section_11_1_sales_relationship", "row_count": len(section_11_relationship_rows)},
        {"section": "section_11_2_sales_action_list", "row_count": len(section_11_action_rows)},
        {"section": "section_11_3_same_admin_new_opportunity", "row_count": len(section_11_new_opportunity_rows)},
    ]
    return {
        "summary": summary_rows,
        "summary_admin": section_11["summary_admin"],
        "opportunity": section_11["opportunity"],
        "pivot_admin_agent": section_11["pivot_admin_agent"],
        "pivot_admin_form_agent": section_11["pivot_admin_form_agent"],
        "sales_relationship": section_sales["relationship"],
        "sales_actions": section_sales["actions"],
        "sales_new_opportunity": section_sales_new_opportunity,
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
    sales_rows = query_sales_rows(project_id=project_id)
    sections = render_report(rows, sales_rows, Path(args.output))

    output_base = Path(args.output).with_suffix("")
    write_csv(sections["summary"], output_base.with_suffix(".csv"))
    write_csv(sections["summary_admin"], output_base.with_name(output_base.name + "_summary_by_admin.csv"))
    write_csv(sections["opportunity"], output_base.with_name(output_base.name + "_opportunity.csv"))
    write_csv(sections["pivot_admin_agent"], output_base.with_name(output_base.name + "_pivot_admin_agent.csv"))
    write_csv(sections["pivot_admin_form_agent"], output_base.with_name(output_base.name + "_pivot_admin_form_agent.csv"))
    write_csv(sections["sales_relationship"], output_base.with_name(output_base.name + "_sales_relationship.csv"))
    write_csv(sections["sales_actions"], output_base.with_name(output_base.name + "_sales_actions.csv"))
    write_csv(sections["sales_new_opportunity"], output_base.with_name(output_base.name + "_sales_new_opportunity.csv"))


if __name__ == "__main__":
    main()
