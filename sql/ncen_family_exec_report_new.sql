-- NCEN Family Executive Report (new) - EA perspective.
-- Uses EA+NCEN enriched view and keeps one latest row per family/fund.
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
    edgar_agents_filings_in_window AS ea_filings_in_window,
    ea_pct_of_company_filings_in_window,
    (COALESCE(edgar_agents_filings_in_window, 0) > 0) AS ever_filed_by_edgar_agents_llc_in_window,
    total_agent_groups_used_in_window,
    all_agent_groups_used_in_window AS agent_groups_used_in_window,
    other_agent_groups_used_in_window,
    filingDate,
    indexDate,
    load_ts
  FROM `sec-edgar-ralph.warplan.v_ea_filings_with_ncen_and_other_agents`
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
ea_fund_form_types AS (
  SELECT
    ncen_family_investment_company_name,
    companyCik,
    STRING_AGG(formType, ', ' ORDER BY filings DESC, formType) AS ea_form_types_for_fund,
    STRING_AGG(CONCAT(formType, '::', CAST(filings AS STRING)), '||' ORDER BY filings DESC, formType) AS ea_form_type_count_pairs
  FROM (
    SELECT
      ncen_family_investment_company_name,
      companyCik,
      formType,
      COUNT(*) AS filings
    FROM base
    WHERE filing_agent_group_normalized IN ('EDGAR AGENTS LLC', 'EDGAR AGENTS, LLC', 'EDGAR AGENTS')
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
  lf.ea_filings_in_window,
  ROUND(lf.ea_pct_of_company_filings_in_window * 100, 2) AS ea_pct_of_company_filings_in_window,
  lf.ever_filed_by_edgar_agents_llc_in_window,
  lf.total_agent_groups_used_in_window,
  lf.agent_groups_used_in_window,
  lf.other_agent_groups_used_in_window,
  fft.ea_form_types_for_fund,
  fft.ea_form_type_count_pairs
FROM latest_fund lf
LEFT JOIN ea_fund_form_types fft
  ON lf.ncen_family_investment_company_name = fft.ncen_family_investment_company_name
 AND lf.companyCik = fft.companyCik
ORDER BY lf.ncen_family_investment_company_name, lf.companyName;
