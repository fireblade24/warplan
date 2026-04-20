WITH sales_book AS (
  SELECT
    CAST(companyCik AS STRING) AS company_cik,
    ANY_VALUE(TRIM(CAST(companyName AS STRING))) AS company_name,
    TRIM(CAST(salesperson_name AS STRING)) AS salesperson_name
  FROM `@sales_table`
  WHERE TRIM(CAST(companyCik AS STRING)) <> ''
    AND TRIM(CAST(salesperson_name AS STRING)) <> ''
  GROUP BY company_cik, salesperson_name
),
latest_ncen AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      CAST(companyCik AS STRING) AS company_cik,
      companyName,
      ncen_family_investment_company_name,
      ncen_admin_names,
      ncen_adviser_names,
      ncen_adviser_types,
      formType,
      filing_agent_group,
      DATE(filingDate) AS filing_date,
      ROW_NUMBER() OVER (
        PARTITION BY CAST(companyCik AS STRING)
        ORDER BY DATE(filingDate) DESC, indexDate DESC, load_ts DESC
      ) AS rn
    FROM `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`
    WHERE TRIM(CAST(companyCik AS STRING)) <> ''
  )
  WHERE rn = 1
),
filing_rollup AS (
  SELECT
    CAST(companyCik AS STRING) AS company_cik,
    COUNT(*) AS filings_in_window,
    COUNT(DISTINCT DATE(filingDate)) AS filing_days,
    STRING_AGG(DISTINCT CAST(formType AS STRING), ', ' ORDER BY CAST(formType AS STRING)) AS form_types,
    STRING_AGG(DISTINCT CAST(filing_agent_group AS STRING), ', ' ORDER BY CAST(filing_agent_group AS STRING)) AS filing_agents_used
  FROM `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`
  WHERE TRIM(CAST(companyCik AS STRING)) <> ''
    AND DATE(filingDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  GROUP BY company_cik
)
SELECT
  sb.salesperson_name,
  sb.company_cik,
  COALESCE(sb.company_name, ln.companyName) AS company_name,
  ln.ncen_family_investment_company_name,
  ln.ncen_admin_names,
  ln.ncen_adviser_names,
  ln.ncen_adviser_types,
  fr.filings_in_window,
  fr.filing_days,
  fr.form_types,
  fr.filing_agents_used,
  ln.filing_date AS last_filing_date
FROM sales_book sb
LEFT JOIN latest_ncen ln USING (company_cik)
LEFT JOIN filing_rollup fr USING (company_cik)
ORDER BY sb.salesperson_name, COALESCE(sb.company_name, ln.companyName), sb.company_cik
