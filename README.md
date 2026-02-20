# warplan

## QUALITY EDGAR SOLUTIONS vendor report

This repo includes a one-row-per-company report pipeline for companies that have used `QUALITY EDGAR SOLUTIONS` as a filing agent. It includes every company with at least one QUALITY EDGAR SOLUTIONS filing, whether or not they are the dominant filer.

### What it produces

For each company, the SQL output includes:
- `companyName`, `companyCIK`
- total filings, QUALITY EDGAR SOLUTIONS filings, and percentage share
- whether QUALITY EDGAR SOLUTIONS is the dominant filer
- matching for QUALITY EDGAR SOLUTIONS is normalized (trim + case-insensitive)
- number of other filing agents used
- when QUALITY EDGAR SOLUTIONS first filed (vendor since)
- most recent filing date and last form type for QUALITY EDGAR SOLUTIONS

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
