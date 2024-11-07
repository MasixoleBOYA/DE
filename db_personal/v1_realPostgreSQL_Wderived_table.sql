with wasted_per_transporter as (
SELECT
    ti.stopdate:: date AS date,
    tc.depot_code,
    tc.depot_location,
    ti.transporter,
    sum(ti.distance) AS total_distance,
    avg(
        CASE
        WHEN (
            100:: numeric:: numeric(18, 0) - tcap.average_capacity_percentage
        ) < 0:: numeric:: numeric(18, 0) THEN 0:: numeric:: numeric(18, 0)
        ELSE 100:: numeric:: numeric(18, 0) - tcap.average_capacity_percentage END
    ) AS percentage_empty,
    sum(
        CASE
        WHEN (
            100:: numeric:: numeric(18, 0) - tcap.average_capacity_percentage
        ) < 0:: numeric:: numeric(18, 0) THEN 0:: numeric:: numeric(18, 0)
        ELSE 100:: numeric:: numeric(18, 0) - tcap.average_capacity_percentage END * (tc.cost_per_km / 100:: numeric:: numeric(18, 0))
    ) AS emptiness_var_cost,
    sum(
        round(
            CASE
            WHEN (
                100:: numeric:: numeric(18, 0) - tcap.average_capacity_percentage
            ) < 0:: numeric:: numeric(18, 0) THEN 0:: numeric:: numeric(18, 0)
            ELSE 100:: numeric:: numeric(18, 0) - tcap.average_capacity_percentage END * (tc.cost_per_km / 100:: numeric:: numeric(18, 0)) * ti.distance:: numeric:: numeric(18, 0),
            2
        )
    ) AS total_amount_wasted_per_transporter,
    ti."type" AS transporter_type,
    tc.contract_ref,
    tc.cost_per_transporter AS monthly_transporter_cost
FROM
    dwh01.ods_ssam_trip_info ti
    JOIN dwh01.i_transporter_contract_costs tc ON ti.vehiclereg:: text = tc.vehiclereg:: text
    JOIN (
        SELECT
            ods_ssam_lnt_truck_cap_optimization_view.date,
            ods_ssam_lnt_truck_cap_optimization_view.tripnum,
            ods_ssam_lnt_truck_cap_optimization_view.depotcode,
            ods_ssam_lnt_truck_cap_optimization_view.transporter,
            ods_ssam_lnt_truck_cap_optimization_view.vehicleregistration,
            ROUND(
        AVG(
            CASE 
                WHEN ods_ssam_lnt_truck_cap_optimization_view.capacity != 0 
                THEN ods_ssam_lnt_truck_cap_optimization_view.volumegst::numeric / ods_ssam_lnt_truck_cap_optimization_view.capacity::numeric * 100
                WHEN ods_ssam_lnt_truck_cap_optimization_view.capacity = 0 then NULL
                ELSE NULL
            END
        )::numeric(18, 2), 
    2) AS average_capacity_percentage
        FROM
            rdm01.ods_ssam_lnt_truck_cap_optimization_view
        GROUP BY
            ods_ssam_lnt_truck_cap_optimization_view.tripnum,
            ods_ssam_lnt_truck_cap_optimization_view.depotcode,
            ods_ssam_lnt_truck_cap_optimization_view.date,
            ods_ssam_lnt_truck_cap_optimization_view.transporter,
            ods_ssam_lnt_truck_cap_optimization_view.vehicleregistration
    ) tcap ON tcap.tripnum:: text = ti.tripnum:: text
WHERE
    ti.tripstatus:: text = 'C':: character varying:: text
    OR ti.tripstatus:: text = 'Closed':: character varying:: text
GROUP BY
    ti.stopdate:: date,
    tc.depot_code,
    tc.depot_location,
    ti.transporter,
    ti."type",
    tc.contract_ref,
    tc.cost_per_transporter

),

CurrentMonth AS (
    SELECT
        transporter,
        depot_location,
        TO_CHAR(date, 'YYYY-MM') AS month,
        date,
        total_amount_wasted_per_transporter,
        SUM(total_amount_wasted_per_transporter) OVER (PARTITION BY transporter, TO_CHAR(date, 'YYYY-MM') ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mtd_waste
    FROM wasted_per_transporter
    WHERE date <= '2024-09-10'
),
PreviousMonth AS (
    SELECT
        transporter,
        depot_location,
        TO_CHAR(date - INTERVAL '30 days', 'YYYY-MM') AS month,
        date,
        total_amount_wasted_per_transporter,
        SUM(total_amount_wasted_per_transporter) OVER (PARTITION BY transporter, TO_CHAR(date - INTERVAL '30 days', 'YYYY-MM') ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mtd_waste
    FROM wasted_per_transporter
    WHERE date <= DATE '2024-09-10' - INTERVAL '30 days'
)

SELECT 
    c.transporter,
    c.depot_location,
    c.month AS current_month,
    c.date AS current_date,
    c.mtd_waste AS mtd_waste_current,
    p.month AS previous_month,
    p.date AS previous_date,
    p.mtd_waste AS mtd_waste_previous
FROM 
    CurrentMonth c
LEFT JOIN 
    PreviousMonth p ON c.transporter = p.transporter 
                   AND EXTRACT(DAY FROM c.date) = EXTRACT(DAY FROM p.date)
                   and c.depot_location = p.depot_location 

WHERE 
    c.month = '2024-09'

GROUP BY c.transporter, c.date, c.mtd_waste, p.month, p.date, p.mtd_waste, c.month, c.depot_location

ORDER BY 
    c.date, c.transporter;
