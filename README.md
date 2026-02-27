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
- one page per non-null `ncen_family_investment_company_name`
- rolls up funds under each family
- shows requested fund-level fields in readable fund cards per family page (not a dense table)
- includes QES form types filed for each fund
- includes AI executive summary per family (openness to switch, potential value to EA, switch-likelihood reasoning, likely problems EA can solve, conversation script)

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
