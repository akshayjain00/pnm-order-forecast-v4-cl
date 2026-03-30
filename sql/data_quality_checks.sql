-- sql/data_quality_checks.sql
-- Pre-flight data quality assertions.
-- Run before every forecast. See spec Section 9.1.
-- Each query returns a single row with check_name and check_result columns.
-- No status filters on orders or opportunities (all statuses included).

-- CHECK 1: Orders table has data for recent dates
-- FAIL if zero intracity PNM orders in last 3 days (suggests data pipeline outage)
SELECT
    'check_orders_recency' AS check_name,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL: No orders in last 3 days'
        ELSE 'PASS'
    END AS check_result
FROM pnm_application.orders o
INNER JOIN pnm_application.shifting_requirements sr
    ON o.sr_id = sr.id
WHERE CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
        >= CURRENT_DATE - 3
    AND sr.shifting_type = 'intra_city'
    AND o.crn ILIKE 'PNM%'
    AND sr.package_name NOT ILIKE '%nano%';

-- CHECK 2: Opportunities exist for each target date
-- Run once per target_date. FAIL if zero opps for that date.
-- Parameterize: :target_date
SELECT
    'check_opps_exist' AS check_name,
    :target_date AS target_date,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL: No opportunities for target date'
        ELSE 'PASS'
    END AS check_result
FROM pnm_application.opportunities opp
INNER JOIN pnm_application.shifting_requirements sr
    ON opp.sr_id = sr.id
WHERE CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) = :target_date
    AND sr.shifting_type = 'intra_city'
    AND sr.package_name NOT ILIKE '%nano%';

-- CHECK 3: No date outliers in shifting_ts
-- WARN if shifting_ts values > 1 year from today exist
SELECT
    'check_date_outliers' AS check_name,
    CASE
        WHEN COUNT(*) > 0 THEN 'WARN: shifting_ts outliers detected'
        ELSE 'PASS'
    END AS check_result
FROM pnm_application.shifting_requirements
WHERE CAST(shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
        > CURRENT_DATE + 365
    AND shifting_type = 'intra_city';

-- CHECK 4: Conversion rate sanity
-- WARN if trailing 14-day conversion is > 1.0 or < 0.01
-- No status filters: total orders / total opportunities
SELECT
    'check_conversion_sanity' AS check_name,
    CASE
        WHEN conv_rate > 1.0 OR conv_rate < 0.01
            THEN 'WARN: Conversion rate out of expected range: '
                 || conv_rate::VARCHAR
        ELSE 'PASS'
    END AS check_result
FROM (
    SELECT
        COUNT(DISTINCT o.id)::FLOAT
            / NULLIF(COUNT(DISTINCT opp.id), 0) AS conv_rate
    FROM pnm_application.opportunities opp
    INNER JOIN pnm_application.shifting_requirements sr
        ON opp.sr_id = sr.id
    LEFT JOIN pnm_application.orders o
        ON o.sr_id = sr.id
        AND o.crn ILIKE 'PNM%'
    WHERE CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
            BETWEEN CURRENT_DATE - 16 AND CURRENT_DATE - 1
        AND sr.shifting_type = 'intra_city'
        AND sr.package_name NOT ILIKE '%nano%'
);
