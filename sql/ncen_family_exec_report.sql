-- NCEN family executive report source query.
-- Pull one latest row per fund within each family where family name is present.
WITH base AS (
  SELECT
    companyCik,
    companyName,
    ncen_family_investment_company_name,
    ncen_investment_company_type,
    ncen_total_series,
    ncen_accession_rows,
    ncen_admin_names,
    ncen_adviser_names,
    ncen_adviser_types,
    total_filings_in_window,
    qes_filings_in_window,
    qes_pct_of_company_filings_in_window,
    ever_filed_by_edgar_agents_llc_in_window,
    total_agent_groups_used_in_window,
    agent_groups_used_in_window,
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
  ncen_family_investment_company_name,
  CAST(companyCik AS STRING) AS companyCik,
  companyName,
  ncen_investment_company_type,
  ncen_total_series,
  ncen_accession_rows,
  ncen_admin_names,
  ncen_adviser_names,
  ncen_adviser_types,
  total_filings_in_window,
  qes_filings_in_window,
  ROUND(qes_pct_of_company_filings_in_window * 100, 2) AS qes_pct_of_company_filings_in_window,
  ever_filed_by_edgar_agents_llc_in_window,
  total_agent_groups_used_in_window,
  agent_groups_used_in_window
FROM latest_fund
ORDER BY ncen_family_investment_company_name, companyName;
