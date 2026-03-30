-- sql/backtest_actuals.sql
-- Pull actual order counts for evaluation.
-- No status filter: all orders included.
-- Parameters:
--   :start_date  DATE  -- First date in evaluation window
--   :end_date    DATE  -- Last date in evaluation window (inclusive)

SELECT
    CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS service_date,
    COUNT(DISTINCT o.id) AS actual_orders
FROM pnm_application.orders o
INNER JOIN pnm_application.shifting_requirements sr
    ON o.sr_id = sr.id
WHERE sr.shifting_type = 'intra_city'
    AND o.crn ILIKE 'PNM%'
    AND sr.package_name NOT ILIKE '%nano%'
    AND CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
        BETWEEN :start_date AND :end_date
GROUP BY 1
ORDER BY 1;
