WITH base AS (
  SELECT
    ncen_family_investment_company_name AS family_name,
    companyCik,
    companyName,
    formType,
    DATE(filingDate) AS filing_date,
    filing_agent_group,
    ncen_admin_names,
    ncen_adviser_names,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(filing_agent_group, '')), r'QUALITY\s+EDGAR\s+SOLUTIONS|\bQES\b') THEN 'QES'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(filing_agent_group, '')), r'EDGAR\s+AGENTS') THEN 'EA'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(filing_agent_group, '')), r'FILE\s*POINT') THEN 'FILE_POINT'
      ELSE NULL
    END AS agent_tag
  FROM `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`
  WHERE TRIM(COALESCE(ncen_family_investment_company_name, '')) <> ''
    AND TRIM(COALESCE(companyName, '')) <> ''
),
fund_agents AS (
  SELECT
    family_name,
    companyCik,
    companyName,
    agent_tag,
    COUNT(*) AS filings,
    STRING_AGG(DISTINCT CAST(formType AS STRING), ', ' ORDER BY CAST(formType AS STRING)) AS forms
  FROM base
  WHERE agent_tag IS NOT NULL
  GROUP BY family_name, companyCik, companyName, agent_tag
),
fund_rollup AS (
  SELECT
    family_name,
    companyCik,
    companyName,
    MAX(IF(agent_tag = 'QES', 1, 0)) = 1 AS has_qes,
    MAX(IF(agent_tag = 'EA', 1, 0)) = 1 AS has_ea,
    MAX(IF(agent_tag = 'FILE_POINT', 1, 0)) = 1 AS has_file_point,
    MAX(IF(agent_tag = 'QES', forms, NULL)) AS qes_forms,
    MAX(IF(agent_tag = 'EA', forms, NULL)) AS ea_forms,
    MAX(IF(agent_tag = 'FILE_POINT', forms, NULL)) AS file_point_forms
  FROM fund_agents
  GROUP BY family_name, companyCik, companyName
),
latest_roles AS (
  SELECT family_name, companyCik, companyName, ncen_admin_names, ncen_adviser_names
  FROM (
    SELECT
      family_name,
      companyCik,
      companyName,
      ncen_admin_names,
      ncen_adviser_names,
      ROW_NUMBER() OVER (
        PARTITION BY family_name, companyCik, companyName
        ORDER BY filing_date DESC
      ) AS rn
    FROM base
  )
  WHERE rn = 1
),
family_presence AS (
  SELECT
    family_name,
    MAX(IF(has_qes, 1, 0)) = 1 AS family_has_qes,
    MAX(IF(has_ea, 1, 0)) = 1 AS family_has_ea,
    MAX(IF(has_file_point, 1, 0)) = 1 AS family_has_file_point,
    COUNT(DISTINCT companyCik) AS family_fund_count
  FROM fund_rollup
  GROUP BY family_name
)
SELECT
  f.family_name,
  f.companyCik,
  f.companyName,
  f.has_qes,
  f.has_ea,
  f.has_file_point,
  f.qes_forms,
  f.ea_forms,
  f.file_point_forms,
  p.family_has_qes,
  p.family_has_ea,
  p.family_has_file_point,
  p.family_fund_count,
  r.ncen_admin_names,
  r.ncen_adviser_names
FROM fund_rollup f
JOIN family_presence p USING (family_name)
LEFT JOIN latest_roles r USING (family_name, companyCik, companyName)
ORDER BY family_name, companyName;
