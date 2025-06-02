/********************************************************************
  2 .  Day-Ahead Capacity tables  (core.*)
*********************************************************************/

CREATE TABLE IF NOT EXISTS core.da_flow_batch (
    batch_id        bigserial PRIMARY KEY,
    market          text,
    delivery_date   date,
    delivery_area   text,
    unit            text,
    version         int,
    status          text,
    updated_at      timestamptz,
    total_import    numeric,
    total_export    numeric,
    total_net_pos   numeric,
    raw             jsonb
);

CREATE TABLE IF NOT EXISTS core.da_flow_hourly (
    batch_id        bigint REFERENCES core.da_flow_batch ON DELETE CASCADE,
    delivery_start  timestamptz,
    delivery_end    timestamptz,
    connection_area text,
    import          numeric,
    export          numeric,
    net_pos         numeric,
    PRIMARY KEY (batch_id, delivery_start)
);



/********************************************************************
  3 .  Parser for this feed only – more can be added later
*********************************************************************/
CREATE OR REPLACE FUNCTION core.parse_da_flow(p_request_id bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_body      jsonb;
    v_batch_id  bigint;
BEGIN
    -- pull the raw body
    SELECT content::jsonb
      INTO v_body
      FROM net._http_response
     WHERE id = p_request_id;


    ----------------------------------------------------------------
    -- 3.1  Insert batch-level row
    ----------------------------------------------------------------
    INSERT INTO core.da_flow_batch (
        market, delivery_date, delivery_area, version, status, unit, total_import, 
        total_export, total_net_pos, updated_at, raw
    )
    SELECT  v_body->>'market',
            (v_body->>'deliveryDateCET')::date,
            v_body->>'deliveryArea',
            COALESCE((v_body->>'version')::int, 1),
            v_body->>'status',
            v_body->>'unit',
            (v_body->>'totalImport')::numeric,
            (v_body->>'totalExport')::numeric,
            (v_body->>'totalNetPosition')::numeric,
            (v_body->>'updatedAt')::timestamptz,
            v_body
    RETURNING batch_id INTO v_batch_id;

    ----------------------------------------------------------------
    -- 3.2  Hourly records
    ----------------------------------------------------------------
    INSERT INTO core.da_flow_hourly
    SELECT  v_batch_id,
            (h->>'deliveryStart')::timestamptz,
            (h->>'deliveryEnd')::timestamptz,
            a.key,							   
            (a.value->>'import' )::numeric,                   
            (a.value->>'export' )::numeric,                   
            (a.value->>'netPosition' )::numeric                   
      FROM  jsonb_array_elements(v_body->'flows') AS h,
            LATERAL jsonb_each(h->'byConnectionArea')  AS a(key,value);
END;
$$;

CREATE OR REPLACE FUNCTION core.build_da_flow_NO2_url(p_date date)
RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT
AS $$
BEGIN
    RETURN format(
      'https://dataportal-api.nordpoolgroup.com/api/DayAheadFlow?date=%s&market=N2EX_DayAhead&deliveryArea=NO2',
      to_char(p_date, 'YYYY-MM-DD')
    );
END;
$$;

CREATE OR REPLACE FUNCTION core.build_da_flow_UK_url(p_date date)
RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT
AS $$
BEGIN
    RETURN format(
      'https://dataportal-api.nordpoolgroup.com/api/DayAheadFlow?date=%s&market=N2EX_DayAhead&deliveryArea=UK',
      to_char(p_date, 'YYYY-MM-DD')
    );
END;
$$;


select ext.api_fetch_all();
select ext.process_api_responses();
select * from core.da_flow_batch;

