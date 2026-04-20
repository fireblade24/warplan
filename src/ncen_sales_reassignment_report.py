#!/usr/bin/env python3
"""Create a reassignment recommendation report for a departing salesperson using NCEN signals."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "sql" / "ncen_sales_reassignment_enrichment.sql"
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


def query_rows(project_id: str, sales_table: str) -> list[dict[str, Any]]:
    sql = SQL_PATH.read_text(encoding="utf-8").replace("@sales_table", sales_table)
    cmd = [
        "bq",
        "query",
        "--project_id",
        project_id,
        "--use_legacy_sql=false",
        "--format=prettyjson",
        "--max_rows=2000000",
    ]
    result = subprocess.run(cmd, input=sql, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "No output from bq CLI"
        raise RuntimeError(f"bq query failed (exit {result.returncode}): {detail}")
    payload = result.stdout.strip()
    if not payload:
        return []
    parsed = json.loads(payload)
    if isinstance(parsed, list):
        return [dict(r) for r in parsed]
    raise RuntimeError("Unexpected bq output format. Expected JSON array.")


def _split_tokens(value: Any) -> set[str]:
    text = str(value or "").strip()
    if not text:
        return set()
    parts = re.split(r"[;,|\n]+", text)
    clean = set()
    for part in parts:
        t = re.sub(r"\s+", " ", part).strip().lower()
        if t:
            clean.add(t)
    return clean


def _as_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _build_sales_books(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    books: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        salesperson = str(row.get("salesperson_name") or "").strip()
        company_cik = str(row.get("company_cik") or "").strip()
        if not salesperson or not company_cik:
            continue
        books[salesperson].append(
            {
                "company_cik": company_cik,
                "company_name": str(row.get("company_name") or "").strip(),
                "family": str(row.get("ncen_family_investment_company_name") or "").strip(),
                "admins": _split_tokens(row.get("ncen_admin_names")),
                "advisers": _split_tokens(row.get("ncen_adviser_names")),
                "adviser_types": _split_tokens(row.get("ncen_adviser_types")),
                "forms": _split_tokens(row.get("form_types")),
                "filing_agents": _split_tokens(row.get("filing_agents_used")),
                "filings_in_window": _as_int(row.get("filings_in_window")),
                "filing_days": _as_int(row.get("filing_days")),
                "last_filing_date": str(row.get("last_filing_date") or "").strip(),
            }
        )
    return books


def _portfolio_features(accounts: list[dict[str, Any]]) -> dict[str, Any]:
    families = {a["family"].lower() for a in accounts if a["family"]}
    admins: set[str] = set()
    advisers: set[str] = set()
    adviser_types: set[str] = set()
    forms: set[str] = set()
    for a in accounts:
        admins |= a["admins"]
        advisers |= a["advisers"]
        adviser_types |= a["adviser_types"]
        forms |= a["forms"]
    return {
        "families": families,
        "admins": admins,
        "advisers": advisers,
        "adviser_types": adviser_types,
        "forms": forms,
        "book_size": len(accounts),
    }


def _confidence(score: float) -> tuple[str, str]:
    if score >= 0.75:
        return "High", "85-95%"
    if score >= 0.55:
        return "Medium", "65-84%"
    return "Low", "0-64%"


def build_reassignment(rows: list[dict[str, Any]], departing_salesperson: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sales_books = _build_sales_books(rows)
    if departing_salesperson not in sales_books:
        raise RuntimeError(
            f"Departing salesperson '{departing_salesperson}' not found in returned sales-book data."
        )

    departing_accounts = sales_books.pop(departing_salesperson)
    active_reps = sorted(sales_books.keys())
    if not active_reps:
        raise RuntimeError("No active salespeople remain after removing departing salesperson.")

    rep_features = {rep: _portfolio_features(accounts) for rep, accounts in sales_books.items()}
    rep_assigned_counts = {rep: 0 for rep in active_reps}

    reassignment_rows: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    for account in sorted(departing_accounts, key=lambda a: (-a["filings_in_window"], a["company_name"])):
        best_rep = ""
        best_score = -1.0
        best_reason = ""

        for rep in active_reps:
            features = rep_features[rep]
            family_overlap = 1.0 if account["family"].lower() in features["families"] and account["family"] else 0.0
            admin_overlap = len(account["admins"] & features["admins"]) / max(1, len(account["admins"]))
            adviser_overlap = len(account["advisers"] & features["advisers"]) / max(1, len(account["advisers"]))
            adviser_type_overlap = len(account["adviser_types"] & features["adviser_types"]) / max(1, len(account["adviser_types"]))
            form_overlap = len(account["forms"] & features["forms"]) / max(1, len(account["forms"]))
            load_balance = 1.0 - ((features["book_size"] + rep_assigned_counts[rep]) / max(1, len(rows)))
            score = (
                0.30 * family_overlap
                + 0.25 * admin_overlap
                + 0.20 * adviser_overlap
                + 0.10 * adviser_type_overlap
                + 0.10 * form_overlap
                + 0.05 * max(0.0, load_balance)
            )
            if score > best_score:
                best_score = score
                best_rep = rep
                best_reason = (
                    f"family_overlap={family_overlap:.2f}, admin_overlap={admin_overlap:.2f}, "
                    f"adviser_overlap={adviser_overlap:.2f}, adviser_type_overlap={adviser_type_overlap:.2f}, "
                    f"form_overlap={form_overlap:.2f}, load_balance={load_balance:.2f}"
                )

        rep_assigned_counts[best_rep] += 1
        confidence_band, confidence_range = _confidence(best_score)
        decision = {
            "departing_salesperson": departing_salesperson,
            "company_cik": account["company_cik"],
            "company_name": account["company_name"],
            "recommended_salesperson": best_rep,
            "score": round(best_score, 4),
            "confidence_band": confidence_band,
            "confidence_range": confidence_range,
            "reasoning": best_reason,
            "family": account["family"],
            "filings_in_window": account["filings_in_window"],
            "filing_days": account["filing_days"],
            "last_filing_date": account["last_filing_date"],
        }
        decisions.append(decision)
        reassignment_rows.append({
            "salesperson_name": best_rep,
            "company_cik": account["company_cik"],
            "company_name": account["company_name"],
            "source": f"Reassigned from {departing_salesperson}",
        })

    return decisions, reassignment_rows


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown_report(decisions: list[dict[str, Any]], output_md: Path, departing_salesperson: str) -> None:
    output_md.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# NCEN Sales Reassignment Report — {departing_salesperson}",
        "",
        "## Method",
        "- Considered all client accounts across all salespeople in the provided sales table.",
        "- Enriched every client with NCEN family, admin, adviser, adviser type, filing velocity, and form mix.",
        "- Ranked each reassignment using weighted fit and load balancing.",
        "",
        "## Recommended Moves",
        "| Company | CIK | Recommended Salesperson | Score | Confidence | Confidence Range | Reasoning |",
        "|---|---:|---|---:|---|---|---|",
    ]
    for d in decisions:
        lines.append(
            f"| {d['company_name']} | {d['company_cik']} | {d['recommended_salesperson']} | {d['score']:.4f} | "
            f"{d['confidence_band']} | {d['confidence_range']} | {d['reasoning']} |"
        )
    output_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_pdf_report(decisions: list[dict[str, Any]], output_pdf: Path, departing_salesperson: str) -> None:
    from weasyprint import HTML

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    if not decisions:
        html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    @page {{ size: Letter portrait; margin: 0.5in; }}
    body {{ font-family: Arial, sans-serif; color: #111827; font-size: 11px; }}
    h1 {{ color: #0f3d63; margin-bottom: 4px; }}
    .empty {{ margin-top: 18px; font-size: 12px; color: #374151; }}
  </style>
</head>
<body>
  <h1>NCEN Sales Reassignment Report</h1>
  <p><b>Departing Salesperson:</b> {html.escape(departing_salesperson)}</p>
  <p class="empty">No reassignment decisions were generated.</p>
</body>
</html>
"""
        HTML(string=html_doc).write_pdf(str(output_pdf))
        return

    decision_rows = []
    for index, d in enumerate(decisions, start=1):
        decision_rows.append(
            f"""
<tr>
  <td>{index}</td>
  <td>{html.escape(d['company_name'])}</td>
  <td>{html.escape(d['company_cik'])}</td>
  <td>{html.escape(d['recommended_salesperson'])}</td>
  <td>{d['score']:.4f}</td>
  <td>{html.escape(d['confidence_band'])}</td>
  <td>{html.escape(d['confidence_range'])}</td>
  <td>{html.escape(str(d['filings_in_window']))}</td>
  <td>{html.escape(str(d['filing_days']))}</td>
  <td>{html.escape(d['last_filing_date'])}</td>
</tr>
<tr class="reasoning-row">
  <td colspan="10"><b>Reasoning:</b> {html.escape(d['reasoning'])}</td>
</tr>
"""
        )

    confidence_counts: dict[str, int] = defaultdict(int)
    for d in decisions:
        confidence_counts[d["confidence_band"]] += 1

    summary_bits = [
        f"Total accounts reassigned: <b>{len(decisions)}</b>",
        f"High confidence: <b>{confidence_counts['High']}</b>",
        f"Medium confidence: <b>{confidence_counts['Medium']}</b>",
        f"Low confidence: <b>{confidence_counts['Low']}</b>",
    ]

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    @page {{ size: Letter landscape; margin: 0.45in; }}
    body {{ font-family: Arial, sans-serif; color: #111827; font-size: 10px; }}
    h1 {{ color: #0f3d63; margin: 0 0 4px 0; font-size: 20px; }}
    .subhead {{ margin: 0 0 8px 0; color: #1f2937; }}
    .summary {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin: 8px 0 12px 0;
      padding: 8px;
      background: #eff6ff;
      border: 1px solid #bfdbfe;
      border-radius: 6px;
      font-size: 10px;
    }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
    thead th {{
      background: #0f3d63;
      color: white;
      font-size: 9px;
      padding: 6px 5px;
      border: 1px solid #d1d5db;
      text-align: left;
    }}
    td {{
      border: 1px solid #e5e7eb;
      padding: 5px;
      vertical-align: top;
      word-wrap: break-word;
      overflow-wrap: anywhere;
    }}
    tbody tr:nth-child(4n+1), tbody tr:nth-child(4n+2) {{ background: #f9fafb; }}
    .reasoning-row td {{
      background: #f3f4f6;
      font-size: 9px;
      color: #374151;
      border-top: none;
    }}
    .method {{
      margin: 0 0 10px 0;
      padding-left: 16px;
    }}
    .method li {{ margin: 2px 0; }}
  </style>
</head>
<body>
  <h1>NCEN Sales Reassignment Report</h1>
  <p class="subhead"><b>Departing Salesperson:</b> {html.escape(departing_salesperson)}</p>
  <ul class="method">
    <li>Analyzes all clients across all salespeople from the supplied sales table.</li>
    <li>Uses NCEN relationship + filing signals (family/admin/adviser/adviser-type/form overlap).</li>
    <li>Ranks recommended owners with weighted fit + load balancing and confidence bands.</li>
  </ul>
  <div class="summary">{' | '.join(summary_bits)}</div>
  <table>
    <thead>
      <tr>
        <th style="width:3%;">#</th>
        <th style="width:18%;">Company</th>
        <th style="width:8%;">CIK</th>
        <th style="width:12%;">Recommended Salesperson</th>
        <th style="width:6%;">Score</th>
        <th style="width:7%;">Confidence</th>
        <th style="width:8%;">Range</th>
        <th style="width:8%;">Filings (365d)</th>
        <th style="width:8%;">Active Days</th>
        <th style="width:12%;">Last Filing Date</th>
      </tr>
    </thead>
    <tbody>
      {''.join(decision_rows)}
    </tbody>
  </table>
</body>
</html>
"""
    HTML(string=html_doc).write_pdf(str(output_pdf))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build NCEN-informed reassignment recommendations for a departing salesperson.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument("--sales-table", required=True, help="BigQuery table containing salesperson_name/companyCik/companyName")
    parser.add_argument("--departing-salesperson", default="Mary Catherine", help="Exact salesperson_name value to reassign")
    parser.add_argument(
        "--output-prefix",
        default=str(OUTPUT_DIR / "ncen_sales_reassignment_report"),
        help="Output prefix used for .csv (detailed), _book.csv (new assignments), .md, and .pdf",
    )
    args = parser.parse_args()

    project_id = _normalized(args.project_id)
    if not _is_effective_value(project_id):
        project_id = _detect_default_project_id()
    if not _is_effective_value(project_id):
        raise SystemExit("BigQuery project is empty. Set --project-id, BQ_PROJECT_ID, or GOOGLE_CLOUD_PROJECT.")

    rows = query_rows(project_id, args.sales_table)
    decisions, reassigned_book = build_reassignment(rows, args.departing_salesperson)

    prefix = Path(args.output_prefix)
    decisions_csv = prefix.with_suffix(".csv")
    reassigned_csv = prefix.with_name(prefix.name + "_book").with_suffix(".csv")
    report_md = prefix.with_suffix(".md")
    report_pdf = prefix.with_suffix(".pdf")

    write_csv(decisions, decisions_csv)
    write_csv(reassigned_book, reassigned_csv)
    write_markdown_report(decisions, report_md, args.departing_salesperson)
    render_pdf_report(decisions, report_pdf, args.departing_salesperson)

    print(f"Created recommendations: {decisions_csv}")
    print(f"Created reassigned sales book: {reassigned_csv}")
    print(f"Created markdown report: {report_md}")
    print(f"Created PDF report: {report_pdf}")


if __name__ == "__main__":
    main()
