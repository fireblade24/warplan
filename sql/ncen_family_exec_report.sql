-- NCEN family executive report source query.
-- Pull one latest row per fund within each family where family name is present.
WITH base AS (
  SELECT
    companyCik,
    companyName,
    formType,
    filing_agent_group,
    UPPER(TRIM(filing_agent_group)) AS filing_agent_group_normalized,
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
),
qes_fund_form_types AS (
  SELECT
    ncen_family_investment_company_name,
    companyCik,
    STRING_AGG(formType, ', ' ORDER BY filings DESC, formType) AS qes_form_types_for_fund
  FROM (
    SELECT
      ncen_family_investment_company_name,
      companyCik,
      formType,
      COUNT(*) AS filings
    FROM base
    WHERE filing_agent_group_normalized = 'QUALITY EDGAR SOLUTIONS'
      AND formType IS NOT NULL
    GROUP BY ncen_family_investment_company_name, companyCik, formType
  )
  GROUP BY ncen_family_investment_company_name, companyCik
)
SELECT
  lf.ncen_family_investment_company_name,
  CAST(lf.companyCik AS STRING) AS companyCik,
  lf.companyName,
  lf.ncen_investment_company_type,
  lf.ncen_total_series,
  lf.ncen_accession_rows,
  lf.ncen_admin_names,
  lf.ncen_adviser_names,
  lf.ncen_adviser_types,
  lf.total_filings_in_window,
  lf.qes_filings_in_window,
  ROUND(lf.qes_pct_of_company_filings_in_window * 100, 2) AS qes_pct_of_company_filings_in_window,
  lf.ever_filed_by_edgar_agents_llc_in_window,
  lf.total_agent_groups_used_in_window,
  lf.agent_groups_used_in_window,
  fft.qes_form_types_for_fund
FROM latest_fund lf
LEFT JOIN qes_fund_form_types fft
  ON lf.ncen_family_investment_company_name = fft.ncen_family_investment_company_name
 AND lf.companyCik = fft.companyCik
ORDER BY lf.ncen_family_investment_company_name, lf.companyName;
