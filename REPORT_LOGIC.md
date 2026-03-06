# QUALITY EDGAR SOLUTIONS Report Logic

This document explains exactly how the current report is built and how each output value is derived.

## 1) Data source

The report reads from:

- BigQuery table: `@project_id.@dataset_id.fact_filing_enriched`.

The Python runner injects `@project_id` and `@dataset_id` into the SQL before running it.

---

## 2) SQL filtering and company inclusion rules

### Base records (`base` CTE)

From `fact_filing_enriched`, the query selects:

- `companyName`
- `companyCIK`
- `formType`
- `filing_agent_group`
- normalized agent name: `UPPER(TRIM(filing_agent_group)) AS filing_agent_group_normalized`
- filing date normalized to date: `DATE(filingDate) AS filing_date`

Base-level filters:

- `companyName IS NOT NULL`
- `filing_agent_group IS NOT NULL`

### Which companies appear in the final report (`qes_companies` CTE)

A company is included if it has **at least one filing** where:

- `filing_agent_group_normalized = 'QUALITY EDGAR SOLUTIONS'`

So this report is one row per company that has used QUALITY EDGAR SOLUTIONS at least once.

---

## 3) Company-level metrics in SQL

### `company_totals` CTE

For each included company (`INNER JOIN qes_companies`):

- `companyCIK`: latest non-null CIK (by filing date), cast to string.
- `total_filings`: count of all filings for the company (all agents).
- `qes_filings`: count where normalized agent equals QUALITY EDGAR SOLUTIONS.
- `qes_percentage`: `qes_filings / total_filings` (as a fraction in SQL, later shown as percent).
- `other_agents_count`: distinct count of non-QES filing agents.

### `agent_breakout` + `agent_ranked`

- Counts filings by company and filing agent.
- Ranks agents by filing volume descending per company.
- Produces:
  - `top_agent_by_volume`
  - `top_agent_filing_count`

### QES-specific timing and form details

#### `qes_dates`

For QUALITY EDGAR SOLUTIONS filings only:

- `qes_vendor_since`: earliest QES filing date (`MIN`).
- `qes_last_filing_date`: latest QES filing date (`MAX`).

#### `qes_last_form`

For QES filings only:

- `qes_last_form_type`: form type from the most recent QES filing.

#### `qes_top_forms`

For QES filings only:

- Count filings by `formType` per company.
- Sort by frequency desc (then form name for tie-break).
- Return top 3 as comma-separated string: `qes_top_3_form_types`.

### Service-length fields in final select

Computed from QES first and last filing dates:

- `qes_service_months`: month difference.
- `qes_service_years`: months / 12 (rounded).
- `qes_service_length`: formatted `Xy Ym`.

---

## 4) Final SQL output columns

The final row per company includes:

- Company identity: `companyName`, `companyCIK`
- Volume/share: `total_filings`, `qes_filings`, `qes_percentage`
- Agent mix: `other_agents_count`, `top_agent_by_volume`, `top_agent_filing_count`
- QES timing: `qes_vendor_since`, `qes_last_filing_date`, service-length fields
- QES form insights: `qes_last_form_type`, `qes_top_3_form_types`

Sorted by:

1. highest QES percentage
2. highest QES filing count
3. company name

---

## 5) Python runner logic

The Python script (`src/quality_edgar_vendor_report.py`):

1. Loads SQL and substitutes project/dataset tokens.
2. Runs the SQL with `bq query` (standard SQL, JSON output, high max rows).
3. Parses rows into Python dictionaries.
4. Applies AI-style scoring per company.
5. Writes PDF (WeasyPrint, landscape) and CSV.

### Project/dataset resolution

- Uses `--project-id` / `--dataset-id` args if passed.
- Falls back to env vars (`BQ_PROJECT_ID`, `BQ_DATASET_ID`), with project fallback to `GOOGLE_CLOUD_PROJECT` or `gcloud config`.
- Treats empty placeholders (e.g., `(unset)`) as missing.

---

## 6) AI-style scoring logic

For each company, scoring uses:

- `total_filings`
- `qes_percentage` (already percent from SQL, e.g. 67.5)
- `other_agents_count`
- `qes_last_form_type`

### Revenue rank (`money_rank`)

Scores from filing volume, QES share, and a complex-form bonus:

- Volume tiers: `>=80`, `>=40`, `>=15`
- Share tiers: `>=70%`, `>=35%`
- Bonus if last QES form matches high-value tags (`S-1`, `S-3`, `10-K`, `10-Q`, `8-K`, `DEF 14A`, `424B`)

Mapped to:

- `$$$$`, `$$$`, `$$`, `$`

### Switching likelihood (`switch_rank`)

Uses:

- lower QES share => higher switch likelihood
- more other agents => higher switch likelihood
- internal dominance heuristic from percentage (`qes_percentage > 85`)

Mapped to:

- `Very Likely`, `Likely`, `Possible`, `Low`, `Very Low`

Also emits `ai_reasoning` text per company.

---

## 7) PDF and CSV output

### PDF

- Generated with WeasyPrint.
- Landscape page layout.
- One table row per company.
- Includes SQL metrics + AI columns.

### CSV

- Same row-level enriched data is written to CSV.

Default output files:

- `output/quality_edgar_vendor_report.pdf`
- `output/quality_edgar_vendor_report.csv`

---

## 8) Important interpretation notes

- The report includes companies with at least one QES filing, not all companies in the table.
- `qes_percentage` is company-level share of QES filings across all filings for that company.
- `top_agent_by_volume` is the largest single filing agent by count; it may be QES or another agent.
- `qes_top_3_form_types` reflects only forms filed by QES for that company.

---

## NCEN Family Executive Report (new) — EA Perspective

This report is separate from existing NCEN outputs and is intentionally EA-first.

### 1) Data source and row scope

Source view:
- `sec-edgar-ralph.warplan.v_ea_filings_with_ncen_and_other_agents`

SQL logic:
- Keeps non-empty `ncen_family_investment_company_name` rows.
- Reduces to latest row per `(family, companyCik)`.
- Builds EA form-type rollups per fund from rows where filing agent matches Edgar Agents aliases.

### 2) Family-level report structure

Python groups rows by family and produces:
- front summary page with all EA form types and filing counts
- AI-tiered family priority list
- one page per family with fund cards

Each fund card includes:
- filing volume and EA share fields
- admin, adviser, adviser type
- filing-agent overlap context (`agent_groups_used_in_window`)
- EA form types for the fund

### 3) AI executive summary (per family)

Per-family summary includes:
- openness to switch
- potential value to EA
- switch-likelihood reasoning
- likely problems EA can solve
- conversation script
- tier classification

### 4) Outputs

Generated by `src/ncen_family_exec_report_new.py`:
- PDF: `output/ncen_family_exec_report_new.pdf`
- Flat CSV: `output/ncen_family_exec_report_new.csv`

---

## NCEN Admin Workload Report (new)

### 1) Data source

- `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`

### 2) Core logic

- Keep records with non-null fund and admin context.
- Compute trailing 365-day window ending at the max `filingDate` in source.
- Compute first-ever filing date per fund (`companyCik`).
- Pick latest record per fund to capture current NCEN admin relationships.
- Split multi-admin values into individual admin rows.

### 3) Admin rollup metrics

For each admin:
- `total_funds`: distinct funds currently mapped to that admin.
- `new_funds_launched_in_window`: distinct funds whose first-ever filing date falls in trailing 365-day window.
- `funds_list`: concatenated list of funds (company name, CIK, family when present).

### 4) Outputs

Generated by `src/ncen_admin_workload_report.py`:
- PDF: `output/ncen_admin_workload_report.pdf`
- CSV: `output/ncen_admin_workload_report.csv`
