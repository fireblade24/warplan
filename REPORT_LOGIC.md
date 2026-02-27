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
