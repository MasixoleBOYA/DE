

select
*
FROM rdm01.ods_ssam_amt_wasted_per_transporter;

------------------------------------ waste per transporter -----------------------
SELECT 
    "date",
--  depot_location,
    transporter,
    total_amount_wasted_per_transporter,
    SUM(total_amount_wasted_per_transporter) OVER (
        PARTITION BY transporter, EXTRACT(MONTH FROM "date")
        ORDER BY "date" 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS month_to_date_total

FROM 
    rdm01.ods_ssam_amt_wasted_per_transporter
--where depot_location = 'Witbank'

group by 
	"date",
	transporter,
	total_amount_wasted_per_transporter
--	depot_location
order by 
    transporter,
"date"
    
-- xxxxxxxxxx
SELECT 
    "date",
    transporter,
    SUM(total_amount_wasted_per_transporter) AS daily_total_wasted_per_transporter,
    SUM(SUM(total_amount_wasted_per_transporter)) OVER (
        PARTITION BY transporter, date_trunc('month', "date")
        ORDER BY "date" 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS month_to_date_total
FROM 
    rdm01.ods_ssam_amt_wasted_per_transporter
GROUP BY 
    "date",
    transporter
ORDER BY 
    transporter,
    "date";

    
    
-------------- depot exceptions split view -------------------
SELECT * FROM rdm01.ods_ssam_lnt_exceptions limit 1;
	
-- xxxxxxxxxxxxxxx without rolling total xxxxxxxxxxxxxxxxxxxxxxxxxx
	
SELECT
    DATE(loaddate) AS date,
    depotcode,
    depotname,
    count(avg_percent) AS depot_split_exceptions
FROM
    rdm01.ods_ssam_lnt_exceptions
--where total_distance != 0 and total_trip_time is not null
GROUP BY
    DATE(loaddate),
    depotcode,
    depotname
ORDER BY
    DATE(loaddate),
    depotcode;
   
-- xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx with rolling total
WITH daily_exceptions AS (
    SELECT
        DATE(loaddate) AS date_only,
        depotcode,
        COUNT(avg_percent) AS daily_exceptions
    FROM
        rdm01.ods_ssam_lnt_exceptions
    GROUP BY
        DATE(loaddate),
        depotcode
)
SELECT
    date_only,
    depotcode,
    daily_exceptions,
    SUM(daily_exceptions) OVER (
        PARTITION BY depotcode, EXTRACT(YEAR FROM date_only), EXTRACT(MONTH FROM date_only)
        ORDER BY date_only
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS rolling_month_to_date_exceptions
FROM
    daily_exceptions
    
ORDER BY
    date_only,
    depotcode;

-- xxxxxxxxxxxx final xxxxxxxxxxxxxxx
SELECT *
FROM 
    rdm01.ods_ssam_lnt_exceptions
where total_distance = 0   
   
   
SELECT 
    DATE(loaddate) AS date,
    depotcode,
    COUNT(avg_percent) AS capacity_exceptions_by_day,
    SUM(COUNT(avg_percent)) OVER (
        PARTITION BY depotcode, DATE_TRUNC('month', DATE(loaddate))
        ORDER BY DATE(loaddate)
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS capacity_exceptions_mtd
FROM 
    rdm01.ods_ssam_lnt_exceptions
--where total_distance != 0 and total_trip_time is not null
GROUP BY 
    DATE(loaddate),
    depotcode
ORDER BY 
--    depotcode,
    DATE(loaddate);

   
-------------------------------------------------
  DISTINCT ti.tripnum,
  od.depotcode,
  ti.transporter,
  oslc.vehicleregistration vehiclereg,
  ti.startdate,
  ti.stopdate,
  oslc.average_capacity_percentage AS avg_percent,
  ti.loaddate:: timestamp with time zone,
  "date_part"(
    'epoch':: character varying:: text,
    ti.stopdate - ti.startdate
  ) AS total_trip_time,
  ti.distance AS total_distance
FROM
  rdm01.ods_ssam_trip_info_view ti
  join rdm01.ods_ssam_lnt_capacity_optimization_per_truck oslc on ti.tripnum = oslc.tripnum 
  and oslc.average_capacity_percentage < 95
  LEFT JOIN (
    SELECT
      CASE
      WHEN od.deliveryvolume:: text ~ '^[0-9]+$':: character varying:: text THEN od.deliveryvolume
      ELSE NULL:: character varying END AS deliveryvol,
      od.station_id,
      od.loadedvolume,
      od.deliveryvolume,
      od.shiptoname,
      od.depotcode,
      od.tripnum,
      od.product_id
    FROM
      rdm01.ods_ssam_order_deliveries_view od
  ) od ON od.tripnum:: text = ti.tripnum:: text with no schema binding;

    
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------


SELECT distinct *
FROM rdm01.ods_ssam_all_kpi_total_exceptions_view;

SELECT *
FROM rdm01.i_depot_view;


