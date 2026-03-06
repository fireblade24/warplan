-- NCEN Admin Workload Report
-- Goal: rank admins by fund coverage and activity.
-- Source: sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles
WITH base AS (
  SELECT
    companyCik,
    companyName,
    ncen_family_investment_company_name,
    ncen_admin_names,
    filingDate,
    indexDate,
    load_ts
  FROM `sec-edgar-ralph.warplan.v_fact_filing_enriched_with_ncen_roles`
  WHERE companyCik IS NOT NULL
    AND companyName IS NOT NULL
    AND ncen_admin_names IS NOT NULL
    AND TRIM(ncen_admin_names) != ''
    AND filingDate IS NOT NULL
),
window_bounds AS (
  SELECT
    MAX(filingDate) AS window_end,
    DATE_SUB(MAX(filingDate), INTERVAL 365 DAY) AS window_start
  FROM base
),
fund_first_seen AS (
  SELECT
    companyCik,
    MIN(filingDate) AS first_filing_date
  FROM base
  GROUP BY companyCik
),
latest_fund AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY companyCik
    ORDER BY filingDate DESC, indexDate DESC, load_ts DESC
  ) = 1
),
fund_admins AS (
  SELECT
    CAST(lf.companyCik AS STRING) AS companyCik,
    lf.companyName,
    lf.ncen_family_investment_company_name,
    TRIM(admin_name) AS admin_name
  FROM latest_fund lf,
  UNNEST(
    SPLIT(
      REGEXP_REPLACE(IFNULL(lf.ncen_admin_names, ''), r'[|\n]+', ';'),
      ';'
    )
  ) AS admin_name
  WHERE TRIM(admin_name) != ''
),
admin_rollup AS (
  SELECT
    fa.admin_name,
    COUNT(DISTINCT fa.companyCik) AS total_funds,
    COUNT(DISTINCT IF(
      ffs.first_filing_date BETWEEN wb.window_start AND wb.window_end,
      fa.companyCik,
      NULL
    )) AS new_funds_launched_in_window,
    STRING_AGG(
      DISTINCT CONCAT(
        fa.companyName,
        ' (',
        fa.companyCik,
        IF(
          fa.ncen_family_investment_company_name IS NOT NULL AND TRIM(fa.ncen_family_investment_company_name) != '',
          CONCAT(' | ', fa.ncen_family_investment_company_name),
          ''
        ),
        ')'
      ),
      '; '
      ORDER BY fa.companyName
    ) AS funds_list
  FROM fund_admins fa
  JOIN fund_first_seen ffs
    ON fa.companyCik = CAST(ffs.companyCik AS STRING)
  CROSS JOIN window_bounds wb
  GROUP BY fa.admin_name
)
SELECT
  admin_name,
  total_funds,
  new_funds_launched_in_window,
  funds_list
FROM admin_rollup
ORDER BY total_funds DESC, new_funds_launched_in_window DESC, admin_name;
