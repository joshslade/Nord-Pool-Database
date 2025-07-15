CREATE OR REPLACE VIEW analytics.latest_da_prices AS
select 
	dpb.delivery_date as delivery_date
    ,TO_CHAR(dph.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dph.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI') AS time_window_gmt
    ,dph.delivery_start
    ,dph.delivery_end
    ,MAX(price) FILTER (WHERE area_code = 'UK') AS uk_gbp
    ,MAX(price) FILTER (WHERE area_code = 'NO2') AS no2_gbp
from core.da_price_hourly dph 
INNER JOIN (
    SELECT delivery_date, MAX(batch_id) AS batch_id
    FROM core.da_price_batch
    GROUP BY delivery_date
) dpb ON dph.batch_id = dpb.batch_id
group by 
	dpb.delivery_date
    ,TO_CHAR(dph.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dph.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI')
    ,dph.delivery_start
    ,dph.delivery_end
order by dph.delivery_start;

CREATE OR REPLACE VIEW analytics.latest_da_idx_prices AS
select 
	dpb.delivery_date as delivery_date
    ,TO_CHAR(dph.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dph.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI') AS time_window_gmt
    ,dph.delivery_start
    ,dph.delivery_end
    ,MAX(price_index) FILTER (WHERE area_code = 'UK') AS uk_gbp
    ,MAX(price_index) FILTER (WHERE area_code = 'NO2') AS no2_gbp
from core.da_idx_price_hourly dph 
INNER JOIN (
    SELECT delivery_date, MAX(batch_id) AS batch_id
    FROM core.da_idx_price_batch
    GROUP BY delivery_date
) dpb ON dph.batch_id = dpb.batch_id
group by 
	dpb.delivery_date
    ,TO_CHAR(dph.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dph.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI')
    ,dph.delivery_start
    ,dph.delivery_end
order by dph.delivery_start;

CREATE OR REPLACE VIEW analytics.latest_da_volumes AS
select 
	dvb.delivery_date as delivery_date
    ,TO_CHAR(dvh.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dvh.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI') AS time_window_gmt
    ,dvh.delivery_start
    ,dvh.delivery_end
    ,MAX(buy_mwh) FILTER (WHERE area_code = 'NO2') AS no2_buy_mwh
    ,MAX(sell_mwh) FILTER (WHERE area_code = 'NO2') AS no2_sell_mwh
    ,MAX(buy_mwh) FILTER (WHERE area_code = 'UK') AS uk_buy_mwh
    ,MAX(sell_mwh) FILTER (WHERE area_code = 'UK') AS uk_sell_mwh
from core.da_vol_hourly dvh 
INNER JOIN (
    SELECT delivery_date, MAX(batch_id) AS batch_id
    FROM core.da_vol_batch
    GROUP BY delivery_date
) dvb ON dvh.batch_id = dvb.batch_id
group by 
	dvb.delivery_date
    ,TO_CHAR(dvh.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dvh.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI')
    ,dvh.delivery_start
    ,dvh.delivery_end
order by dvh.delivery_start;

CREATE OR REPLACE VIEW analytics.latest_da_capacity AS
select 
	 dcb.delivery_date as delivery_date
    ,TO_CHAR(dch.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dch.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI') AS time_window_gmt
    ,dch.delivery_start
    ,dch.delivery_end
    ,max(dch.import_value) as no2_to_uk_mwh
    ,max(dch.export_value) as uk_to_no2_mwh
from core.da_capacity_hourly dch 
INNER JOIN (
    SELECT delivery_date, MAX(batch_id) AS batch_id
    FROM core.da_capacity_batch
    GROUP BY delivery_date
) dcb ON dch.batch_id = dcb.batch_id
group by 
	dcb.delivery_date
    ,TO_CHAR(dch.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dch.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI')
    ,dch.delivery_start
    ,dch.delivery_end
order by dch.delivery_start;

CREATE OR REPLACE VIEW analytics.latest_da_flow AS
select 
	 dfb.delivery_date as delivery_date
    ,TO_CHAR(dfh.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dfh.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI') AS time_window_gmt
    ,dfh.delivery_start
    ,dfh.delivery_end
    ,MAX(export) FILTER (WHERE delivery_area = 'NO2') as no2_total_export_mwh
    ,MAX(import) FILTER (WHERE delivery_area = 'NO2') as no2_total_import_mwh
    ,MAX(export) FILTER (WHERE delivery_area = 'UK') as uk_total_export_mwh
    ,MAX(import) FILTER (WHERE delivery_area = 'UK') as uk_total_import_mwh
from core.da_flow_hourly dfh 
INNER JOIN (
    SELECT delivery_date, delivery_area, MAX(batch_id) AS batch_id
    FROM core.da_flow_batch
    GROUP BY delivery_date, delivery_area
) dfb ON dfh.batch_id = dfb.batch_id
group by 
	dfb.delivery_date
    ,TO_CHAR(dfh.delivery_start AT TIME ZONE 'Europe/London', 'HH24:MI') || ' - ' || 
	 TO_CHAR(dfh.delivery_end   AT TIME ZONE 'Europe/London', 'HH24:MI')
    ,dfh.delivery_start
    ,dfh.delivery_end
order by dfh.delivery_start;

CREATE OR REPLACE VIEW analytics.latest_da_combined AS
SELECT
    p.delivery_date,
    p.time_window_gmt,
    p.delivery_start,
    p.delivery_end,
    p.uk_gbp,
    p.no2_gbp,
    ip.uk_gbp AS uk_idx_gbp,
    ip.no2_gbp AS no2_idx_gbp,
    v.no2_buy_mwh,
    v.no2_sell_mwh,
    v.uk_buy_mwh,
    v.uk_sell_mwh,
    c.no2_to_uk_mwh,
    c.uk_to_no2_mwh,
    f.no2_total_export_mwh,
    f.no2_total_import_mwh,
    f.uk_total_export_mwh,
    f.uk_total_import_mwh
FROM analytics.latest_da_prices p
LEFT JOIN analytics.latest_da_idx_prices ip
    ON p.delivery_date = ip.delivery_date
    AND p.time_window_gmt = ip.time_window_gmt
    AND p.delivery_start = ip.delivery_start
    AND p.delivery_end = ip.delivery_end
LEFT JOIN analytics.latest_da_volumes v
    ON p.delivery_date = v.delivery_date
    AND p.time_window_gmt = v.time_window_gmt
    AND p.delivery_start = v.delivery_start
    AND p.delivery_end = v.delivery_end
LEFT JOIN analytics.latest_da_capacity c
    ON p.delivery_date = c.delivery_date
    AND p.time_window_gmt = c.time_window_gmt
    AND p.delivery_start = c.delivery_start
    AND p.delivery_end = c.delivery_end
LEFT JOIN analytics.latest_da_flow f
    ON p.delivery_date = f.delivery_date
    AND p.time_window_gmt = f.time_window_gmt
    AND p.delivery_start = f.delivery_start
    AND p.delivery_end = f.delivery_end
ORDER BY p.delivery_start;