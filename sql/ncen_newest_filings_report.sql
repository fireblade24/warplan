-- 10 newest filings report
-- Source: sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles
SELECT
  filingDate,
  indexDate,
  load_ts,
  companyName,
  CAST(companyCik AS STRING) AS companyCik,
  formType,
  accessionNumber,
  filingTxtUrl,
  filingIndexUrl,
  filing_agent_group,
  agent_category,
  agent_category_refined,
  is_filing_agent,
  is_self_filer,
  ncen_registrant_name,
  ncen_file_num,
  ncen_family_investment_company_name,
  ncen_investment_company_type,
  ncen_total_series,
  ncen_accession_rows,
  ncen_admin_names,
  ncen_adviser_names,
  ncen_adviser_types
FROM `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`
WHERE filingDate IS NOT NULL
ORDER BY filingDate DESC, indexDate DESC, load_ts DESC
LIMIT 10;
