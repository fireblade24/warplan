#!/usr/bin/env python3
"""Generate NCEN admin workload report (PDF + CSV)."""

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
SQL_PATH = ROOT / "sql" / "ncen_admin_workload_report.sql"
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


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def render_pdf(rows: list[dict[str, Any]], output_pdf: Path) -> None:
    from weasyprint import HTML

    total_admins = len(rows)
    total_funds = sum(_to_int(r.get("total_funds")) for r in rows)
    total_new_funds = sum(_to_int(r.get("new_funds_launched_in_window")) for r in rows)

    body_rows = []
    for row in rows:
        body_rows.append(
            "<tr>"
            f"<td>{html.escape(_display(row.get('admin_name')))}</td>"
            f"<td>{html.escape(_display(row.get('total_funds')))}</td>"
            f"<td>{html.escape(_display(row.get('new_funds_launched_in_window')))}</td>"
            f"<td>{html.escape(_display(row.get('funds_list')))}</td>"
            "</tr>"
        )

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <style>
    @page {{ size: Letter landscape; margin: 0.35in; }}
    body {{ font-family: Arial, sans-serif; font-size: 10px; color: #1c1c1c; }}
    h1 {{ font-size: 20px; margin: 0 0 6px 0; border-bottom: 2px solid #0c4a6e; padding-bottom: 4px; }}
    .summary {{ background: #f8fafc; border: 1px solid #cbd5e1; padding: 8px; margin: 0 0 10px 0; }}
    .summary p {{ margin: 2px 0; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
    th, td {{ border: 1px solid #d1d5db; padding: 4px; vertical-align: top; word-wrap: break-word; }}
    th {{ background: #f1f5f9; text-align: left; }}
  </style>
</head>
<body>
  <h1>NCEN Admin Workload Report</h1>
  <div class="summary">
    <p><b>Total Admins:</b> {total_admins}</p>
    <p><b>Total Funds Covered (sum across admins):</b> {total_funds}</p>
    <p><b>New Funds Launched In Window (sum across admins):</b> {total_new_funds}</p>
  </div>
  <table>
    <thead>
      <tr>
        <th style="width: 18%;">Admin</th>
        <th style="width: 8%;">Total Funds</th>
        <th style="width: 12%;">New Funds In Window</th>
        <th style="width: 62%;">Funds</th>
      </tr>
    </thead>
    <tbody>
      {''.join(body_rows)}
    </tbody>
  </table>
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
    parser = argparse.ArgumentParser(description="Generate NCEN admin workload report.")
    parser.add_argument("--project-id", default=os.getenv("BQ_PROJECT_ID"), required=False)
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "ncen_admin_workload_report.pdf"),
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
