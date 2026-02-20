-- Vendor performance report for QUALITY EDGAR SOLUTIONS.
-- Replace @project_id and @dataset_id in the calling script.
WITH base AS (
  SELECT
    companyName,
    companyCIK,
    formType,
    filing_agent_group,
    DATE(filingDate) AS filing_date
  FROM `@project_id.@dataset_id.fact_filing_enriched`
  WHERE companyName IS NOT NULL
    AND filing_agent_group IS NOT NULL
),
qes_companies AS (
  SELECT DISTINCT companyName
  FROM base
  WHERE filing_agent_group = 'QUALITY EDGAR SOLUTIONS'
),
company_totals AS (
  SELECT
    b.companyName,
    COALESCE(
      CAST(ARRAY_AGG(b.companyCIK IGNORE NULLS ORDER BY b.filing_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS STRING),
      ""
    ) AS companyCIK,
    COUNT(*) AS total_filings,
    COUNTIF(b.filing_agent_group = 'QUALITY EDGAR SOLUTIONS') AS qes_filings,
    SAFE_DIVIDE(
      COUNTIF(b.filing_agent_group = 'QUALITY EDGAR SOLUTIONS'),
      COUNT(*)
    ) AS qes_percentage,
    COUNT(DISTINCT IF(b.filing_agent_group != 'QUALITY EDGAR SOLUTIONS', b.filing_agent_group, NULL)) AS other_agents_count
  FROM base b
  INNER JOIN qes_companies qc USING (companyName)
  GROUP BY b.companyName
),
agent_breakout AS (
  SELECT
    b.companyName,
    b.filing_agent_group,
    COUNT(*) AS filings_by_agent
  FROM base b
  INNER JOIN qes_companies qc USING (companyName)
  GROUP BY b.companyName, b.filing_agent_group
),
agent_ranked AS (
  SELECT
    companyName,
    filing_agent_group,
    filings_by_agent,
    ROW_NUMBER() OVER (
      PARTITION BY companyName
      ORDER BY filings_by_agent DESC, filing_agent_group
    ) AS agent_rank
  FROM agent_breakout
),
qes_dates AS (
  SELECT
    b.companyName,
    MIN(b.filing_date) AS qes_vendor_since,
    MAX(b.filing_date) AS qes_last_filing_date
  FROM base b
  WHERE b.filing_agent_group = 'QUALITY EDGAR SOLUTIONS'
  GROUP BY b.companyName
),
qes_last_form AS (
  SELECT
    b.companyName,
    ARRAY_AGG(b.formType ORDER BY b.filing_date DESC LIMIT 1)[OFFSET(0)] AS qes_last_form_type
  FROM base b
  WHERE b.filing_agent_group = 'QUALITY EDGAR SOLUTIONS'
  GROUP BY b.companyName
)
SELECT
  ct.companyName,
  ct.companyCIK,
  ct.total_filings,
  ct.qes_filings,
  ROUND(ct.qes_percentage * 100, 2) AS qes_percentage,
  ct.other_agents_count,
  qd.qes_vendor_since,
  qd.qes_last_filing_date,
  qlf.qes_last_form_type,
  IF(ar.filing_agent_group = 'QUALITY EDGAR SOLUTIONS', TRUE, FALSE) AS is_qes_dominant_filer,
  ar.filing_agent_group AS top_agent_by_volume,
  ar.filings_by_agent AS top_agent_filing_count
FROM company_totals ct
LEFT JOIN qes_dates qd USING (companyName)
LEFT JOIN qes_last_form qlf USING (companyName)
LEFT JOIN agent_ranked ar
  ON ct.companyName = ar.companyName
 AND ar.agent_rank = 1
ORDER BY ct.qes_percentage DESC, ct.qes_filings DESC, ct.companyName;
