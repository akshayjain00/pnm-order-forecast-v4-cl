-- sql/forecast_snapshot.sql
-- Unified forecast snapshot for the hybrid model.
-- Extends base_signals.sql with:
--   * booked_share_by_cutoff  (SIGNAL 4: nowcast fraction)
--   * is_peak_date            (last 2 days of month OR day = 1)
--   * is_month_edge           (day <= 3 OR days_to_end <= 2)
--   * 5 DQ columns
--
-- Parameters (same as base_signals.sql):
--   :eval_date             DATE   -- Simulated "today" (CURRENT_DATE for prod)
--   :backtest_mode         BOOL   -- TRUE for backtesting, FALSE for production
--   :run_hour              INT    -- Hour of day for as-of cutoff (0-23, IST)
--   :opp_volume_lower_pct  FLOAT  -- Lower bound for similar-volume matching (default 0.90)
--   :opp_volume_upper_pct  FLOAT  -- Upper bound for similar-volume matching (default 1.20)
--
-- Output: One row per (target_date, horizon) with all signals + DQ flags.
-- NOTE: No status filters on orders or opportunities (all statuses included).

WITH dates AS (
    -- Next 3 service dates from eval_date
    SELECT
        DATEADD(DAY, seq4(), :eval_date::DATE) AS target_date,
        seq4() AS horizon
    FROM TABLE(GENERATOR(ROWCOUNT => 3))
),

-- SIGNAL 1: Confirmed orders floor (spec Section 3, Signal 1)
-- All orders for target date, no status filter
floor_orders AS (
    SELECT
        d.target_date,
        d.horizon,
        COUNT(DISTINCT o.id) AS floor_orders
    FROM dates d
    INNER JOIN pnm_application.shifting_requirements sr
        ON CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) = d.target_date
    INNER JOIN pnm_application.orders o
        ON o.sr_id = sr.id
    WHERE sr.shifting_type = 'intra_city'
        AND o.crn ILIKE 'PNM%'
        AND sr.package_name NOT ILIKE '%nano%'
        -- Backtest as-of filter: only orders created before run_hour of eval_date
        AND (
            NOT :backtest_mode
            OR o.created_at < :eval_date::DATE + INTERVAL ':run_hour hours'
        )
    GROUP BY d.target_date, d.horizon
),

-- SIGNAL 2: Pipeline by bucket (spec Section 3, Signal 2)
-- All opportunities included (no status filter)
pipeline_buckets AS (
    SELECT
        d.target_date,
        d.horizon,
        CASE
            WHEN DATEDIFF(DAY,
                CAST(opp.created_at + INTERVAL '5 hours, 30 minutes' AS DATE),
                d.target_date) <= 1 THEN 0  -- B0: 0-1 days
            WHEN DATEDIFF(DAY,
                CAST(opp.created_at + INTERVAL '5 hours, 30 minutes' AS DATE),
                d.target_date) <= 3 THEN 1  -- B1: 2-3 days
            WHEN DATEDIFF(DAY,
                CAST(opp.created_at + INTERVAL '5 hours, 30 minutes' AS DATE),
                d.target_date) <= 7 THEN 2  -- B2: 4-7 days
            ELSE 3                           -- B3: 8+ days
        END AS bucket,
        COUNT(DISTINCT opp.id) AS opp_count
    FROM dates d
    INNER JOIN pnm_application.shifting_requirements sr
        ON CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) = d.target_date
    INNER JOIN pnm_application.opportunities opp
        ON opp.sr_id = sr.id
    WHERE sr.shifting_type = 'intra_city'
        AND sr.package_name NOT ILIKE '%nano%'
        -- Backtest as-of filter
        AND (
            NOT :backtest_mode
            OR opp.created_at < :eval_date::DATE + INTERVAL ':run_hour hours'
        )
    GROUP BY d.target_date, d.horizon, bucket
),

-- Pivot pipeline buckets to one row per (target_date, horizon)
pipeline_pivoted AS (
    SELECT
        target_date,
        horizon,
        COALESCE(SUM(CASE WHEN bucket = 0 THEN opp_count END), 0) AS open_opps_b0,
        COALESCE(SUM(CASE WHEN bucket = 1 THEN opp_count END), 0) AS open_opps_b1,
        COALESCE(SUM(CASE WHEN bucket = 2 THEN opp_count END), 0) AS open_opps_b2,
        COALESCE(SUM(CASE WHEN bucket = 3 THEN opp_count END), 0) AS open_opps_b3,
        COALESCE(SUM(opp_count), 0) AS total_open_opps
    FROM pipeline_buckets
    GROUP BY target_date, horizon
),

-- Pre-compute per-bucket conversion for historical service dates
hist_bucket_raw AS (
    SELECT
        CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS service_date,
        DAYOFWEEK(sr.shifting_ts + INTERVAL '5 hours, 30 minutes') AS dow,
        CASE
            WHEN DATEDIFF(DAY,
                CAST(opp.created_at + INTERVAL '5 hours, 30 minutes' AS DATE),
                CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)) <= 1 THEN 0
            WHEN DATEDIFF(DAY,
                CAST(opp.created_at + INTERVAL '5 hours, 30 minutes' AS DATE),
                CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)) <= 3 THEN 1
            WHEN DATEDIFF(DAY,
                CAST(opp.created_at + INTERVAL '5 hours, 30 minutes' AS DATE),
                CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)) <= 7 THEN 2
            ELSE 3
        END AS bucket,
        COUNT(DISTINCT opp.id) AS bucket_opps,
        COUNT(DISTINCT o.id) AS bucket_orders,
        COUNT(DISTINCT o.id)::FLOAT
            / NULLIF(COUNT(DISTINCT opp.id), 0) AS conv_rate
    FROM pnm_application.opportunities opp
    INNER JOIN pnm_application.shifting_requirements sr
        ON opp.sr_id = sr.id
    LEFT JOIN pnm_application.orders o
        ON o.sr_id = sr.id
        AND o.crn ILIKE 'PNM%'
    WHERE sr.shifting_type = 'intra_city'
        AND sr.package_name NOT ILIKE '%nano%'
        AND CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
            < :eval_date::DATE
        AND CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
            >= DATEADD(WEEK, -8, :eval_date::DATE)
    GROUP BY 1, 2, 3
),

-- Historical conversion rates by bucket (spec Section 3, Signal 2)
historical_bucket_conv AS (
    SELECT
        d.target_date,
        d.horizon,
        hb.bucket,
        COALESCE(
            SUM(hb.conv_rate
                * (1.0 / (DATEDIFF(DAY, hb.service_date, d.target_date) + 1)))
            / NULLIF(
                SUM(1.0 / (DATEDIFF(DAY, hb.service_date, d.target_date) + 1)),
                0),
            (
                SELECT
                    COUNT(DISTINCT o2.id)::FLOAT
                        / NULLIF(COUNT(DISTINCT opp2.id), 0)
                FROM pnm_application.opportunities opp2
                INNER JOIN pnm_application.shifting_requirements sr2
                    ON opp2.sr_id = sr2.id
                LEFT JOIN pnm_application.orders o2
                    ON o2.sr_id = sr2.id
                    AND o2.crn ILIKE 'PNM%'
                WHERE CAST(sr2.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
                    BETWEEN DATEADD(DAY, -14, d.target_date)
                        AND DATEADD(DAY, -1, d.target_date)
                    AND sr2.shifting_type = 'intra_city'
                    AND sr2.package_name NOT ILIKE '%nano%'
            )
        ) AS conv_rate
    FROM dates d
    CROSS JOIN hist_bucket_raw hb
    LEFT JOIN pipeline_buckets pb
        ON pb.target_date = d.target_date
        AND pb.horizon = d.horizon
        AND pb.bucket = hb.bucket
    WHERE hb.service_date < d.target_date
        AND (
            DAY(d.target_date) >= DAYOFMONTH(LAST_DAY(d.target_date)) - 1
            OR hb.dow = DAYOFWEEK(d.target_date)
        )
        AND hb.bucket_opps BETWEEN
            COALESCE(pb.opp_count, 0) * :opp_volume_lower_pct
            AND
            COALESCE(pb.opp_count, 0) * :opp_volume_upper_pct
    GROUP BY d.target_date, d.horizon, hb.bucket
),

-- Pivot conversion rates to one row per (target_date, horizon)
conv_pivoted AS (
    SELECT
        target_date,
        horizon,
        COALESCE(MAX(CASE WHEN bucket = 0 THEN conv_rate END), 0.10) AS conv_rate_b0,
        COALESCE(MAX(CASE WHEN bucket = 1 THEN conv_rate END), 0.10) AS conv_rate_b1,
        COALESCE(MAX(CASE WHEN bucket = 2 THEN conv_rate END), 0.10) AS conv_rate_b2,
        COALESCE(MAX(CASE WHEN bucket = 3 THEN conv_rate END), 0.05) AS conv_rate_b3
    FROM historical_bucket_conv
    GROUP BY target_date, horizon
),

-- SIGNAL 3: Seasonal baseline (spec Section 3, Signal 3)
-- 10-week weekday average (excludes peak dates)
ten_week AS (
    SELECT
        d.target_date,
        d.horizon,
        AVG(hist.order_count) AS ten_week_avg
    FROM dates d
    INNER JOIN (
        SELECT
            CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS order_date,
            DAYOFWEEK(sr.shifting_ts + INTERVAL '5 hours, 30 minutes') AS dow,
            COUNT(DISTINCT o.id) AS order_count
        FROM pnm_application.orders o
        INNER JOIN pnm_application.shifting_requirements sr
            ON o.sr_id = sr.id
        WHERE o.crn ILIKE 'PNM%'
            AND sr.shifting_type = 'intra_city'
            AND sr.package_name NOT ILIKE '%nano%'
        GROUP BY 1, 2
    ) hist
        ON hist.dow = DAYOFWEEK(d.target_date)
        AND hist.order_date BETWEEN DATEADD(DAY, -70, d.target_date)
                                AND DATEADD(DAY, -7, d.target_date)
        AND DAY(hist.order_date) < (
            SELECT DAYOFMONTH(LAST_DAY(hist.order_date)) - 1
        )
    GROUP BY d.target_date, d.horizon
),

-- 12-month same-date-of-month average (excludes peak dates)
twelve_month AS (
    SELECT
        d.target_date,
        d.horizon,
        AVG(hist.order_count) AS twelve_month_avg
    FROM dates d
    INNER JOIN (
        SELECT
            CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS order_date,
            DAY(sr.shifting_ts + INTERVAL '5 hours, 30 minutes') AS dom,
            DATE_TRUNC('MONTH', sr.shifting_ts + INTERVAL '5 hours, 30 minutes')
                AS order_month,
            COUNT(DISTINCT o.id) AS order_count,
            ROW_NUMBER() OVER (
                PARTITION BY DAY(sr.shifting_ts + INTERVAL '5 hours, 30 minutes')
                ORDER BY DATE_TRUNC('MONTH',
                    sr.shifting_ts + INTERVAL '5 hours, 30 minutes') DESC
            ) AS rn
        FROM pnm_application.orders o
        INNER JOIN pnm_application.shifting_requirements sr
            ON o.sr_id = sr.id
        WHERE o.crn ILIKE 'PNM%'
            AND sr.shifting_type = 'intra_city'
            AND sr.package_name NOT ILIKE '%nano%'
        GROUP BY 1, 2, 3
    ) hist
        ON hist.dom = DAY(d.target_date)
        AND hist.order_date < d.target_date
        AND hist.rn <= 12
        AND hist.dom < DAYOFMONTH(LAST_DAY(hist.order_date)) - 1
    GROUP BY d.target_date, d.horizon
),

-- Peak multiplier: ratio of historical peak-day orders to normal-day orders
peak_stats AS (
    SELECT
        AVG(CASE WHEN DAY(order_date) >= DAYOFMONTH(LAST_DAY(order_date)) - 1 THEN order_count END)
            / NULLIF(AVG(CASE WHEN DAY(order_date) < DAYOFMONTH(LAST_DAY(order_date)) - 1 THEN order_count END), 0)
                AS peak_multiplier
    FROM (
        SELECT
            CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS order_date,
            COUNT(DISTINCT o.id) AS order_count
        FROM pnm_application.orders o
        INNER JOIN pnm_application.shifting_requirements sr
            ON o.sr_id = sr.id
        WHERE o.crn ILIKE 'PNM%'
            AND sr.shifting_type = 'intra_city'
            AND sr.package_name NOT ILIKE '%nano%'
            AND CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
                >= DATEADD(MONTH, -12, :eval_date::DATE)
        GROUP BY 1
    )
),

-- SIGNAL 4: Nowcast — booked share by cutoff hour
-- Historical median: what fraction of final orders are booked by run_hour IST?
booked_share_hist AS (
    SELECT
        d.target_date,
        d.horizon,
        MEDIAN(
            booked_so_far.order_count::FLOAT
            / NULLIF(final_orders.order_count, 0)
        ) AS booked_share_by_cutoff
    FROM dates d
    CROSS JOIN (
        -- Historical days with same DOW
        SELECT
            CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS service_date,
            DAYOFWEEK(sr.shifting_ts + INTERVAL '5 hours, 30 minutes') AS dow
        FROM pnm_application.shifting_requirements sr
        WHERE sr.shifting_type = 'intra_city'
            AND sr.package_name NOT ILIKE '%nano%'
            AND CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
                BETWEEN DATEADD(WEEK, -8, :eval_date::DATE)
                AND DATEADD(DAY, -1, :eval_date::DATE)
        GROUP BY 1, 2
    ) hist_days
    INNER JOIN (
        -- Orders booked before run_hour on each historical day
        SELECT
            CAST(sr2.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS service_date,
            COUNT(DISTINCT o2.id) AS order_count
        FROM pnm_application.orders o2
        INNER JOIN pnm_application.shifting_requirements sr2 ON o2.sr_id = sr2.id
        WHERE o2.crn ILIKE 'PNM%'
            AND sr2.shifting_type = 'intra_city'
            AND sr2.package_name NOT ILIKE '%nano%'
            AND CAST(o2.created_at + INTERVAL '5 hours, 30 minutes' AS TIME)
                < TIMEADD('hour', :run_hour, '00:00:00'::TIME)
        GROUP BY 1
    ) booked_so_far ON booked_so_far.service_date = hist_days.service_date
    INNER JOIN (
        -- Final orders for each historical day
        SELECT
            CAST(sr3.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS service_date,
            COUNT(DISTINCT o3.id) AS order_count
        FROM pnm_application.orders o3
        INNER JOIN pnm_application.shifting_requirements sr3 ON o3.sr_id = sr3.id
        WHERE o3.crn ILIKE 'PNM%'
            AND sr3.shifting_type = 'intra_city'
            AND sr3.package_name NOT ILIKE '%nano%'
        GROUP BY 1
    ) final_orders ON final_orders.service_date = hist_days.service_date
    WHERE hist_days.dow = DAYOFWEEK(d.target_date)
    GROUP BY d.target_date, d.horizon
)

-- FINAL OUTPUT: One row per (target_date, horizon)
SELECT
    d.target_date,
    d.horizon,
    COALESCE(f.floor_orders, 0)                 AS floor_orders,
    COALESCE(p.open_opps_b0, 0)                 AS open_opps_b0,
    COALESCE(p.open_opps_b1, 0)                 AS open_opps_b1,
    COALESCE(p.open_opps_b2, 0)                 AS open_opps_b2,
    COALESCE(p.open_opps_b3, 0)                 AS open_opps_b3,
    COALESCE(p.total_open_opps, 0)              AS total_open_opps,
    COALESCE(c.conv_rate_b0, 0.10)              AS conv_rate_b0,
    COALESCE(c.conv_rate_b1, 0.10)              AS conv_rate_b1,
    COALESCE(c.conv_rate_b2, 0.10)              AS conv_rate_b2,
    COALESCE(c.conv_rate_b3, 0.05)              AS conv_rate_b3,
    COALESCE(tw.ten_week_avg, 0)                AS ten_week_avg,
    COALESCE(tm.twelve_month_avg, 0)            AS twelve_month_avg,
    COALESCE(ps.peak_multiplier, 1.0)           AS peak_multiplier,

    -- SIGNAL 4: Nowcast booked share
    bsh.booked_share_by_cutoff,

    -- Calendar flags (hybrid peak definition)
    CASE
        WHEN DAY(d.target_date) >= DAYOFMONTH(LAST_DAY(d.target_date)) - 1
            OR DAY(d.target_date) = 1
        THEN TRUE
        ELSE FALSE
    END AS is_peak_date,

    -- Month-edge flag (day <= 3 OR days_to_end <= 2)
    CASE
        WHEN DAY(d.target_date) <= 3
            OR DAY(d.target_date) >= DAYOFMONTH(LAST_DAY(d.target_date)) - 2
        THEN TRUE
        ELSE FALSE
    END AS is_month_edge,

    -- DQ CHECK 1: floor must be non-negative
    CASE
        WHEN COALESCE(f.floor_orders, 0) < 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_floor_nonneg,

    -- DQ CHECK 2: booked share should not exceed 1.0
    CASE
        WHEN bsh.booked_share_by_cutoff > 1.0 THEN 'WARN'
        ELSE 'PASS'
    END AS dq_share_bound,

    -- DQ CHECK 3: seasonal signal should be non-zero for T+0
    CASE
        WHEN COALESCE(tw.ten_week_avg, 0) = 0 AND d.horizon = 0 THEN 'WARN'
        ELSE 'PASS'
    END AS dq_seasonal_found,

    -- DQ CHECK 4: pipeline should exist for T+0
    CASE
        WHEN COALESCE(p.total_open_opps, 0) = 0 AND d.horizon = 0 THEN 'WARN'
        ELSE 'PASS'
    END AS dq_pipeline_exists,

    -- DQ CHECK 5: conversion rates should not all be at defaults (0.10/0.10/0.10/0.05)
    CASE
        WHEN COALESCE(c.conv_rate_b0, 0.10) = 0.10
            AND COALESCE(c.conv_rate_b1, 0.10) = 0.10
            AND COALESCE(c.conv_rate_b2, 0.10) = 0.10
            AND COALESCE(c.conv_rate_b3, 0.05) = 0.05
        THEN 'WARN'
        ELSE 'PASS'
    END AS dq_conv_not_all_defaults

FROM dates d
LEFT JOIN floor_orders f
    ON d.target_date = f.target_date AND d.horizon = f.horizon
LEFT JOIN pipeline_pivoted p
    ON d.target_date = p.target_date AND d.horizon = p.horizon
LEFT JOIN conv_pivoted c
    ON d.target_date = c.target_date AND d.horizon = c.horizon
LEFT JOIN ten_week tw
    ON d.target_date = tw.target_date AND d.horizon = tw.horizon
LEFT JOIN twelve_month tm
    ON d.target_date = tm.target_date AND d.horizon = tm.horizon
LEFT JOIN booked_share_hist bsh
    ON d.target_date = bsh.target_date AND d.horizon = bsh.horizon
CROSS JOIN peak_stats ps
ORDER BY d.horizon;
