# warplan

## QUALITY EDGAR SOLUTIONS vendor report

This repo includes a one-row-per-company report pipeline for companies that have used `QUALITY EDGAR SOLUTIONS` as a filing agent. It includes every company with at least one QUALITY EDGAR SOLUTIONS filing, whether or not they are the dominant filer.

### What it produces

For each company, the SQL output includes:
- `companyName`, `companyCIK`
- total filings, QUALITY EDGAR SOLUTIONS filings, and percentage share
- top 3 most popular form types QUALITY EDGAR SOLUTIONS files for the company
- matching for QUALITY EDGAR SOLUTIONS is normalized (trim + case-insensitive)
- number of other filing agents used
- when QUALITY EDGAR SOLUTIONS first filed (vendor since)
- most recent filing date and service length (months/years) from first QES filing to last QES filing
- last form type for QUALITY EDGAR SOLUTIONS

Then an AI scoring layer adds:
- money potential: `$`, `$$`, `$$$`, `$$$$`
- switching likelihood: `Very Low`, `Low`, `Possible`, `Likely`, `Very Likely`
- short reasoning text per company

### Files

- SQL logic: `sql/quality_edgar_vendor_report.sql`
- Job runner (BigQuery + AI scoring + WeasyPrint): `src/quality_edgar_vendor_report.py`

### Run (same style as prior project)

Assuming your environment already has dependencies installed and the `bq` CLI is available/authenticated. The script reads `BQ_PROJECT_ID` and `BQ_DATASET_ID`; if project is missing, it falls back to `GOOGLE_CLOUD_PROJECT` or `gcloud config get-value project`. Empty placeholder values like `(unset)` are treated as missing.

Note: `pandas`, `google-cloud-bigquery`, and `jinja2` are **not required** anymore for this script.

```bash
python src/quality_edgar_vendor_report.py
```

Optional explicit args:

```bash
python src/quality_edgar_vendor_report.py \
  --project-id <your-gcp-project> \
  --dataset-id <your-bq-dataset> \
  --output output/quality_edgar_vendor_report.pdf
```

Outputs:
- `output/quality_edgar_vendor_report.pdf`
- `output/quality_edgar_vendor_report.csv`


## NCEN Family Executive Report (new)

Additional report using:
- `sec-edgar-ralph.warplan.v_qes_filings_20250101_20260224_enriched`

What it does:
- front summary page with all QES form types + total filing counts across dataset, EA-overlap fund count, and AI-tiered family priority list
- one page per non-null `ncen_family_investment_company_name`
- rolls up funds under each family
- shows requested fund-level fields in readable fund cards per family page (not a dense table)
- includes QES form types filed for each fund
- includes AI executive summary per family (tier, openness to switch, potential value to EA, switch-likelihood reasoning, likely problems EA can solve, conversation script)

Files:
- SQL logic: `sql/ncen_family_exec_report.sql`
- Runner: `src/ncen_family_exec_report.py`

Run:
```bash
python src/ncen_family_exec_report.py
```

Optional:
```bash
python src/ncen_family_exec_report.py \
  --project-id <your-gcp-project> \
  --output output/ncen_family_exec_report.pdf
```

Outputs:
- `output/ncen_family_exec_report.pdf`
- `output/ncen_family_exec_report.csv`


## NCEN Relationship Network Report (admin/adviser leverage)

Additional report focused on relationship-based expansion paths using shared administrators and advisers.

What it does:
- rolls up registrants by **admin**
- in a separate section, rolls up registrants by **adviser**
- groups families by shared **admin + adviser** pairs
- flags overlap where registrants show both EA (`ever_filed_by_edgar_agents_llc_in_window`) and QES (`qes_filings_in_window > 0`)
- creates a leverage network section highlighting admins/advisers where EA+QES overlap exists and there are additional QES-only registrants to target
- includes family-name lists wherever applicable, including families QES files for and families EA files for

Files:
- SQL logic: `sql/ncen_relationship_network_report.sql`
- Runner: `src/ncen_relationship_network_report.py`

Run:
```bash
python src/ncen_relationship_network_report.py
```

Optional:
```bash
python src/ncen_relationship_network_report.py \
  --project-id <your-gcp-project> \
  --output output/ncen_relationship_network_report.pdf
```

Outputs:
- `output/ncen_relationship_network_report.pdf`
- `output/ncen_relationship_network_report_leverage_network.csv`
- `output/ncen_relationship_network_report_by_admin.csv`
- `output/ncen_relationship_network_report_by_adviser.csv`
- `output/ncen_relationship_network_report_admin_adviser_pairs.csv`

## NCEN Family Executive Report (new) — EA Perspective

This is a separate report (does not replace existing NCEN reports) based on:
- `sec-edgar-ralph.warplan.v_ea_filings_with_ncen_and_other_agents`

What it does:
- front summary page with all EA form types + filing counts across dataset
- AI-tiered family priority list
- one page per non-null `ncen_family_investment_company_name`
- readable fund cards per family (not a dense table)
- includes EA form types filed for each fund
- includes admin/adviser fields and filing-agent overlap context
- includes AI executive summary per family

Files:
- SQL logic: `sql/ncen_family_exec_report_new.sql`
- Runner: `src/ncen_family_exec_report_new.py`

Run:
```bash
python src/ncen_family_exec_report_new.py
```

Optional:
```bash
python src/ncen_family_exec_report_new.py   --project-id <your-gcp-project>   --output output/ncen_family_exec_report_new.pdf
```

Outputs:
- `output/ncen_family_exec_report_new.pdf`
- `output/ncen_family_exec_report_new.csv`

## NCEN Admin Workload Report (new)

Brand-new report to map which administrators have the largest fund workload and who is most active in launching new funds.

Source:
- `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`
- Date window: filings from `2025-01-01` through `CURRENT_DATE()`

What it does:
- rolls up by admin
- shows total distinct funds handled by each admin
- shows list of funds handled by each admin
- adds `new_funds_launched_in_window` (first-ever filing date for the fund falls in trailing 365-day window ending at max filing date in dataset)
- sorts by largest fund workload first

Files:
- SQL logic: `sql/ncen_admin_workload_report.sql`
- Runner: `src/ncen_admin_workload_report.py`

Run:
```bash
python src/ncen_admin_workload_report.py
```

Optional:
```bash
python src/ncen_admin_workload_report.py \
  --project-id <your-gcp-project> \
  --output output/ncen_admin_workload_report.pdf
```

Outputs:
- `output/ncen_admin_workload_report.pdf`
- `output/ncen_admin_workload_report.csv`

## NCEN Multi-Agent Fund Family Report (new)

New multi-section report requested to compare QES, Edgar Agents (EA), and File Point coverage at fund-family, fund, admin, and adviser levels.

Source:
- `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`
- Date window: filings from `2025-01-01` through `CURRENT_DATE()`

What it does:
- Section 1: all fund families where QES appears (3-column list)
- Section 2: all fund families where File Point appears (3-column list)
- Section 3: fund families in common between QES and EA (3-column list)
- Section 4: fund families in common between File Point and EA (3-column list)
- Section 5: fund families in common between QES, EA, and File Point (3-column list)
- Section 6a: common QES+EA+File Point families with fund-level admin, form lists, and agent presence flags
- Section 6b: QES+EA families with fund-level admin, form lists, and agent presence flags
- Section 6c: File Point+EA families with fund-level admin, form lists, and agent presence flags
- Section 7: all admins with counts of distinct funds worked by QES, EA, and File Point
- Section 8: all advisers with counts of distinct funds worked by QES, EA, and File Point
- Section 9: all admins with exact fund list and QES/EA/File Point flags per fund
- Section 10: all advisers with exact fund list and QES/EA/File Point flags per fund
- Section 11.1: hierarchical clean table (`Admin -> Fund Family -> Fund -> Form Type -> Filing Agent`)
- Section 11.2: summary by admin with filing-agent distribution counts and share (`EA`, `QES`, `FilePoint`, `Other`)
- Section 11.3: opportunity table by admin/family with EA presence, competitor mix, opportunity type, fund counts, and high-value filing counts
- Every section restarts page numbering and prints `Page N of X` inside the section
- Agent normalization includes `QES`, `EA`, `FilePoint`, `DFIN` aliases, and `Other`

Files:
- SQL logic: `sql/ncen_multi_agent_fund_family_report.sql`
- Runner: `src/ncen_multi_agent_fund_family_report.py`

Run:
```bash
python src/ncen_multi_agent_fund_family_report.py
```

Optional:
```bash
python src/ncen_multi_agent_fund_family_report.py \
  --project-id <your-gcp-project> \
  --output output/ncen_multi_agent_fund_family_report.pdf
```

Outputs:
- `output/ncen_multi_agent_fund_family_report.pdf`
- `output/ncen_multi_agent_fund_family_report.csv`
- `output/ncen_multi_agent_fund_family_report_section11_clean.csv`
- `output/ncen_multi_agent_fund_family_report_section11_summary_by_admin.csv`
- `output/ncen_multi_agent_fund_family_report_section11_opportunity.csv`
- `output/ncen_multi_agent_fund_family_report_section11_pivot_admin_agent.csv`
- `output/ncen_multi_agent_fund_family_report_section11_pivot_admin_form_agent.csv`


## Action Plan QES/FilePoint Report (new)

Focused action-plan report built from selected sections of the NCEN Multi-Agent Fund Family Report.

Source:
- `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`
- Date window: filings from `2025-01-01` through `CURRENT_DATE()`

What it does:
- Section 1: all fund families where QES appears (3-column list)
- Section 2: all fund families where File Point appears (3-column list)
- Section 3: fund families in common between QES and EA (3-column list)
- Section 4: fund families in common between File Point and EA (3-column list)
- Section 5: fund families in common between QES, EA, and File Point (3-column list)
- Section 6: QES+EA+File Point common families with fund-level admin, form lists, and agent presence flags
- Section 7: QES+EA families with fund-level admin, form lists, and agent presence flags
- Section 8: File Point+EA families with fund-level admin, form lists, and agent presence flags
- Section 9: summary by admin with filing-agent distribution counts and share (`EA`, `QES`, `FilePoint`, `Other`)
- Section 10: opportunity table by admin/family with EA presence, competitor mix, opportunity type, fund counts, and high-value filing counts
- Section 11.1: sales person relationship table using `sec-edgar-ralph.warplan.client_list`, matching first on fund family and then falling back to fund name, while staying inside the same QES/FilePoint/EA universe
- Section 11.2: sales person action list grouped into `Expansion`, `Defend`, and `New`, broken into a separate page section for each sales person, with reasons naming the competing filer and `Form Types Available` limited to the forms EA does not file yet
- Section 11.3: same-admin new-opportunity assignment table showing families/funds where EA is not present but QES and/or FilePoint are, assigned to the sales person who already has an EA relationship in that admin group
- Every section restarts page numbering and prints `Page N of X` inside the section

Files:
- Runner: `src/action_plan_qes_filepoint.py`
- Reused SQL logic: `sql/ncen_multi_agent_fund_family_report.sql`

Run:
```bash
python src/action_plan_qes_filepoint.py
```

Optional:
```bash
python src/action_plan_qes_filepoint.py   --project-id <your-gcp-project>   --output output/action_plan_qes_filepoint.pdf
```

Outputs:
- `output/action_plan_qes_filepoint.pdf`
- `output/action_plan_qes_filepoint.csv`
- `output/action_plan_qes_filepoint_summary_by_admin.csv`
- `output/action_plan_qes_filepoint_opportunity.csv`
- `output/action_plan_qes_filepoint_pivot_admin_agent.csv`
- `output/action_plan_qes_filepoint_pivot_admin_form_agent.csv`
- `output/action_plan_qes_filepoint_sales_relationship.csv`
- `output/action_plan_qes_filepoint_sales_actions.csv`
- `output/action_plan_qes_filepoint_sales_new_opportunity.csv`
