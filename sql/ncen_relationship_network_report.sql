-- NCEN relationship network source rows.
-- One latest row per registrant/fund with family, admin, adviser, and vendor usage signals.
WITH base AS (
  SELECT
    companyCik,
    companyName,
    filing_agent_group,
    UPPER(TRIM(filing_agent_group)) AS filing_agent_group_normalized,
    ncen_family_investment_company_name,
    ncen_admin_names,
    ncen_adviser_names,
    total_filings_in_window,
    qes_filings_in_window,
    qes_pct_of_company_filings_in_window,
    ever_filed_by_edgar_agents_llc_in_window,
    filingDate,
    indexDate,
    load_ts
  FROM `sec-edgar-ralph.warplan.v_qes_filings_20250101_20260224_enriched`
  WHERE ncen_family_investment_company_name IS NOT NULL
    AND TRIM(ncen_family_investment_company_name) != ''
),
latest_fund AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ncen_family_investment_company_name, companyCik
    ORDER BY filingDate DESC, indexDate DESC, load_ts DESC
  ) = 1
)
SELECT
  CAST(companyCik AS STRING) AS companyCik,
  companyName,
  ncen_family_investment_company_name,
  ncen_admin_names,
  ncen_adviser_names,
  total_filings_in_window,
  qes_filings_in_window,
  ROUND(qes_pct_of_company_filings_in_window * 100, 2) AS qes_pct_of_company_filings_in_window,
  ever_filed_by_edgar_agents_llc_in_window
FROM latest_fund
ORDER BY ncen_family_investment_company_name, companyName;
