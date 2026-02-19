# warplan

## QUALITY EDGAR SOLUTIONS vendor report

This repo includes a new report pipeline that builds a one-row-per-company PDF and CSV for companies that have used `QUALITY EDGAR SOLUTIONS` as a filing agent.

### What it produces

For each company, the SQL output includes:
- `companyName`, `companyCIK`
- total filings, QUALITY EDGAR SOLUTIONS filings, and percentage share
- whether QUALITY EDGAR SOLUTIONS is the dominant filer
- number of other filing agents used
- when QUALITY EDGAR SOLUTIONS first filed (vendor since)
- most recent filing date and last form type for QUALITY EDGAR SOLUTIONS

Then an AI scoring layer adds:
- money potential: `$`, `$$`, `$$$`, `$$$$`
- switching likelihood: `Very Low`, `Low`, `Possible`, `Likely`, `Very Likely`
- short reasoning text per company

### Files

- SQL logic: `sql/quality_edgar_vendor_report.sql`
- Report generator (BigQuery + AI scoring + WeasyPrint): `reports/quality_edgar_vendor_report.py`

### Run

```bash
pip install google-cloud-bigquery pandas jinja2 weasyprint
python reports/quality_edgar_vendor_report.py \
  --project-id <your-gcp-project> \
  --dataset-id <your-bq-dataset>
```

Outputs:
- `output/quality_edgar_vendor_report.pdf`
- `output/quality_edgar_vendor_report.csv`
