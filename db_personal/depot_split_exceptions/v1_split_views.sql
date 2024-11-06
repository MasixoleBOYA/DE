

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
SELECT * FROM rdm01.ods_ssam_lnt_exceptions;


select
	loaddate as date,
	--	count(distinct tripnum) over(partition by cast(startdate as date)) as distinct_exceptions_by_day,
	--	count(avg_percent) over(partition by cast(loaddate as date)) as exceptions_by_day,
	--	sum(distinct exceptions_by_day) over(partition by extract(month from STARTDATE)),
	--	tripnum,
	depotcode,
--	transporter,
	count(avg_percent) over(partition by depotcode) as cap_exceptions_by_day
	--	vehiclereg,
	--	startdate,
	--	stopdate,
	--	avg_percent,
	--	loaddate,
	--	total_trip_time,
	--	total_distance
from
	rdm01.ods_ssam_lnt_exceptions

----------------------------------------------
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
FROM rdm01.ods_ssam_lnt_exceptions;

SELECT *
FROM rdm01.i_depot_view;


