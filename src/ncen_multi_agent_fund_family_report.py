#!/usr/bin/env python3
"""Generate a multi-section NCEN report for QES/EA/File Point family overlap."""

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
SQL_PATH = ROOT / "sql" / "ncen_multi_agent_fund_family_report.sql"
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


def _is_true(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


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


def _chunked(rows: list[list[str]], size: int) -> list[list[list[str]]]:
    if not rows:
        return [[]]
    return [rows[i : i + size] for i in range(0, len(rows), size)]


def _table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body or '<tr><td colspan=\"99\">No rows</td></tr>'}</tbody></table>"


def _format_three_column_list(items: list[str]) -> list[list[str]]:
    rows: list[list[str]] = []
    for i in range(0, len(items), 3):
        batch = items[i : i + 3]
        rows.append(batch + [""] * (3 - len(batch)))
    return rows


def _render_section_pages(section_num: int, title: str, subtitle: str, headers: list[str], rows: list[list[str]], rows_per_page: int = 25) -> str:
    chunks = _chunked(rows, rows_per_page)
    total_pages = len(chunks)
    pages = []
    for idx, chunk in enumerate(chunks, start=1):
        pages.append(
            "\n".join(
                [
                    '<section class="page">',
                    f"<h1>Section {section_num}: {html.escape(title)}</h1>",
                    f"<p>{html.escape(subtitle)}</p>",
                    _table(headers, chunk),
                    f'<div class="page-number">Page {idx} of {total_pages}</div>',
                    "</section>",
                ]
            )
        )
    return "\n".join(pages)


def _prepare_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    fund_rows = []
    families_qes: set[str] = set()
    families_fp: set[str] = set()
    families_qes_ea: set[str] = set()
    families_fp_ea: set[str] = set()
    families_qes_ea_fp: set[str] = set()

    for row in rows:
        family = str(row.get("family_name") or "").strip()
        if not family:
            continue
        family_has_qes = _is_true(row.get("family_has_qes"))
        family_has_ea = _is_true(row.get("family_has_ea"))
        family_has_fp = _is_true(row.get("family_has_file_point"))
        if family_has_qes:
            families_qes.add(family)
        if family_has_fp:
            families_fp.add(family)
        if family_has_qes and family_has_ea:
            families_qes_ea.add(family)
        if family_has_fp and family_has_ea:
            families_fp_ea.add(family)
        if family_has_qes and family_has_ea and family_has_fp:
            families_qes_ea_fp.add(family)

        fund_rows.append(
            {
                "family": family,
                "fund": str(row.get("companyName") or "").strip(),
                "has_qes": _is_true(row.get("has_qes")),
                "has_ea": _is_true(row.get("has_ea")),
                "has_fp": _is_true(row.get("has_file_point")),
                "qes_forms": str(row.get("qes_forms") or "").strip(),
                "ea_forms": str(row.get("ea_forms") or "").strip(),
                "fp_forms": str(row.get("file_point_forms") or "").strip(),
                "admins": _split_entities(row.get("ncen_admin_names")),
                "advisers": _split_entities(row.get("ncen_adviser_names")),
            }
        )

    admin_rollup: dict[str, dict[str, Any]] = {}
    adviser_rollup: dict[str, dict[str, Any]] = {}

    for fr in fund_rows:
        fund_key = f"{fr['family']} :: {fr['fund']}"
        for admin in fr["admins"]:
            slot = admin_rollup.setdefault(
                admin,
                {"qes": set(), "ea": set(), "fp": set(), "all": set(), "fund_agents": []},
            )
            slot["all"].add(fund_key)
            if fr["has_qes"]:
                slot["qes"].add(fund_key)
            if fr["has_ea"]:
                slot["ea"].add(fund_key)
            if fr["has_fp"]:
                slot["fp"].add(fund_key)
            slot["fund_agents"].append((fund_key, fr["has_qes"], fr["has_ea"], fr["has_fp"]))

        for adviser in fr["advisers"]:
            slot = adviser_rollup.setdefault(
                adviser,
                {"qes": set(), "ea": set(), "fp": set(), "all": set(), "fund_agents": []},
            )
            slot["all"].add(fund_key)
            if fr["has_qes"]:
                slot["qes"].add(fund_key)
            if fr["has_ea"]:
                slot["ea"].add(fund_key)
            if fr["has_fp"]:
                slot["fp"].add(fund_key)
            slot["fund_agents"].append((fund_key, fr["has_qes"], fr["has_ea"], fr["has_fp"]))

    return {
        "fund_rows": fund_rows,
        "families_qes": sorted(families_qes),
        "families_fp": sorted(families_fp),
        "families_qes_ea": sorted(families_qes_ea),
        "families_fp_ea": sorted(families_fp_ea),
        "families_qes_ea_fp": sorted(families_qes_ea_fp),
        "admin_rollup": admin_rollup,
        "adviser_rollup": adviser_rollup,
    }


def render_report(rows: list[dict[str, Any]], output_pdf: Path) -> dict[str, list[dict[str, Any]]]:
    from weasyprint import HTML

    prepared = _prepare_rows(rows)
    fund_rows = prepared["fund_rows"]

    section_1_rows = _format_three_column_list(prepared["families_qes"])
    section_2_rows = _format_three_column_list(prepared["families_fp"])
    section_3_rows = _format_three_column_list(prepared["families_qes_ea"])
    section_4_rows = _format_three_column_list(prepared["families_fp_ea"])
    section_5_rows = _format_three_column_list(prepared["families_qes_ea_fp"])

    section_6_rows = []
    for fr in sorted(fund_rows, key=lambda x: (x["family"], x["fund"])):
        if fr["family"] not in prepared["families_qes_ea_fp"]:
            continue
        section_6_rows.append(
            [
                fr["family"],
                fr["fund"],
                "; ".join(fr["admins"]) or "-",
                "Y" if fr["has_qes"] else "N",
                fr["qes_forms"] or "-",
                "Y" if fr["has_ea"] else "N",
                fr["ea_forms"] or "-",
                "Y" if fr["has_fp"] else "N",
                fr["fp_forms"] or "-",
            ]
        )

    admin_rows = []
    admin_fund_rows = []
    for admin, slot in sorted(prepared["admin_rollup"].items()):
        admin_rows.append([admin, str(len(slot["qes"])), str(len(slot["ea"])), str(len(slot["fp"])), str(len(slot["all"]))])
        uniq = sorted(set(slot["fund_agents"]))
        for fund_key, has_qes, has_ea, has_fp in uniq:
            admin_fund_rows.append([admin, fund_key, "Y" if has_qes else "N", "Y" if has_ea else "N", "Y" if has_fp else "N"])

    adviser_rows = []
    adviser_fund_rows = []
    for adviser, slot in sorted(prepared["adviser_rollup"].items()):
        adviser_rows.append([adviser, str(len(slot["qes"])), str(len(slot["ea"])), str(len(slot["fp"])), str(len(slot["all"]))])
        uniq = sorted(set(slot["fund_agents"]))
        for fund_key, has_qes, has_ea, has_fp in uniq:
            adviser_fund_rows.append([adviser, fund_key, "Y" if has_qes else "N", "Y" if has_ea else "N", "Y" if has_fp else "N"])

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <style>
    @page {{ size: Letter landscape; margin: 0.4in; }}
    body {{ font-family: Arial, sans-serif; font-size: 10px; color: #111827; }}
    .page {{ page-break-after: always; position: relative; min-height: 7in; }}
    .page:last-child {{ page-break-after: auto; }}
    h1 {{ font-size: 18px; margin: 0 0 6px 0; border-bottom: 2px solid #0c4a6e; padding-bottom: 4px; }}
    p {{ margin: 2px 0 8px 0; color: #334155; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; margin-bottom: 10px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 4px; text-align: left; vertical-align: top; word-wrap: break-word; }}
    th {{ background: #f1f5f9; }}
    .page-number {{ position: absolute; bottom: 0; right: 0; font-size: 10px; color: #475569; }}
  </style>
</head>
<body>
  {_render_section_pages(1, 'All Fund Families where QES is a client filing agent', 'Unique fund families with QES presence.', ['Fund Family', 'Fund Family', 'Fund Family'], section_1_rows)}
  {_render_section_pages(2, 'All Fund Families where File Point appears', 'Unique fund families with File Point presence.', ['Fund Family', 'Fund Family', 'Fund Family'], section_2_rows)}
  {_render_section_pages(3, 'Fund Families in Common: QES and EA', 'Families where both QES and EA file at least one fund.', ['Fund Family', 'Fund Family', 'Fund Family'], section_3_rows)}
  {_render_section_pages(4, 'Fund Families in Common: File Point and EA', 'Families where both File Point and EA file at least one fund.', ['Fund Family', 'Fund Family', 'Fund Family'], section_4_rows)}
  {_render_section_pages(5, 'Fund Families in Common: QES, EA, and File Point', 'Families where all three filing agents are present.', ['Fund Family', 'Fund Family', 'Fund Family'], section_5_rows)}
  {_render_section_pages(6, 'QES + EA + File Point Common Families with Forms by Fund', 'Shows admin, forms each agent files, and whether each agent files each fund.', ['Fund Family', 'Fund', 'Admin(s)', 'QES Files?', 'QES Forms', 'EA Files?', 'EA Forms', 'File Point Files?', 'File Point Forms'], section_6_rows, rows_per_page=16)}
  {_render_section_pages(7, 'Admins: How Many Funds Each Agent Works With', 'Counts of distinct funds per admin by filing agent.', ['Admin', 'QES Funds', 'EA Funds', 'File Point Funds', 'All Distinct Funds'], admin_rows)}
  {_render_section_pages(8, 'Advisers: How Many Funds Each Agent Works With', 'Counts of distinct funds per adviser by filing agent.', ['Adviser', 'QES Funds', 'EA Funds', 'File Point Funds', 'All Distinct Funds'], adviser_rows)}
  {_render_section_pages(9, 'Admins: Which Funds Work With QES, EA, and File Point', 'Fund-by-fund agent presence by admin.', ['Admin', 'Fund (Family :: Fund)', 'QES', 'EA', 'File Point'], admin_fund_rows, rows_per_page=20)}
  {_render_section_pages(10, 'Advisers: Which Funds Work With QES, EA, and File Point', 'Fund-by-fund agent presence by adviser.', ['Adviser', 'Fund (Family :: Fund)', 'QES', 'EA', 'File Point'], adviser_fund_rows, rows_per_page=20)}
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
        {"section": "section_6_fund_forms", "row_count": len(section_6_rows)},
        {"section": "section_7_admin_counts", "row_count": len(admin_rows)},
        {"section": "section_8_adviser_counts", "row_count": len(adviser_rows)},
        {"section": "section_9_admin_funds", "row_count": len(admin_fund_rows)},
        {"section": "section_10_adviser_funds", "row_count": len(adviser_fund_rows)},
    ]
    return {"summary": summary_rows}


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
    parser = argparse.ArgumentParser(description="Generate NCEN multi-agent fund family report.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "ncen_multi_agent_fund_family_report.pdf"),
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
    write_csv(sections["summary"], Path(args.output).with_suffix(".csv"))


if __name__ == "__main__":
    main()
