select delivery_area, count(*), min(delivery_date) as min_date, max(delivery_date) as max_date from core.da_flow_batch dfb group by delivery_area;
select
  area_code,
  COUNT(*) AS row_count,
  MIN(delivery_start) AS min_date,
  MAX(delivery_start) AS max_date,
  (MAX(delivery_start)::date - MIN(delivery_start)::date) AS num_days,
  COUNT(*) / NULLIF((MAX(delivery_start)::date - MIN(delivery_start)::date), 0) AS hours_per_day
FROM core.da_price_hourly
GROUP BY area_code;


select * from core.da_flow_hourly dfb

select * from ext.api_endpoint ae 

with src as (select arl.*,
  substring(url FROM 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})') AS delivery_date,
  substring(url FROM 'deliveryArea=([^&]+)') AS delivery_area
from ext.api_request_log arl where endpoint_id in (4,5))
select
	success, delivery_area, count(*), min(delivery_date) as min_date, max(delivery_date) as max_date from src group by success, delivery_area

select ext.process_api_responses()


SELECT 
  substring(url FROM 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})') AS delivery_date,
  substring(url FROM 'deliveryArea=([^&]+)') AS delivery_area
FROM (VALUES
  ('https://dataportal-api.nordpoolgroup.com/api/DayAheadFlow?date=2025-04-16&market=N2EX_DayAhead&deliveryArea=UK')
) AS t(url);


TRUNCATE TABLE core.da_flow_hourly, core.da_flow_batch RESTART IDENTITY CASCADE;
select * from ext.api_request_log arl where endpoint_id in (4,5)

select * from ext.api_endpoint ae 

select * from net._http_response hr 

select 
arl.endpoint_id,
hr.* 
from net._http_response hr 
join ext.api_request_log arl on arl.request_id = hr.id


select * from core.da_flow_batch dfb 