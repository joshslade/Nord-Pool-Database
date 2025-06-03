/********************************************************************
  2 .  Day-Ahead Capacity tables  (core.*)
*********************************************************************/

-- header-level (one row per API call)
CREATE TABLE IF NOT EXISTS core.da_capacity_batch (
    batch_id        bigserial PRIMARY KEY,
    market          text       NOT NULL,
    delivery_date   date       NOT NULL,
    delivery_area   text       NOT NULL,
    unit            text       NOT NULL,
    updated_at      timestamptz NOT NULL,
    total_import    numeric,
    total_export    numeric,
    raw             jsonb      NOT NULL
);

-- hourly totals
CREATE TABLE IF NOT EXISTS core.da_capacity_hourly (
    batch_id        bigint      REFERENCES core.da_capacity_batch ON DELETE CASCADE,
    delivery_start  timestamptz NOT NULL,
    delivery_end    timestamptz NOT NULL,
    import_connection_area text        NOT NULL,
    import_value    numeric,
    export_connection_area text        NOT NULL,
    export_value    numeric,
    PRIMARY KEY (batch_id, delivery_start)
);


/********************************************************************
  3 .  Parser for this feed only – more can be added later
*********************************************************************/
CREATE OR REPLACE FUNCTION core.parse_da_capacity(p_request_id bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_body      jsonb;
    v_batch_id  bigint;
    v_delivery_dt  date;
    v_count        integer;
BEGIN
    -- pull the raw body
    SELECT content::jsonb
      INTO v_body
      FROM net._http_response
     WHERE id = p_request_id;

    -- 2) extract delivery_date so we can log it
    v_delivery_dt := (v_body->>'deliveryDateCET')::date;

    ----------------------------------------------------------------
    -- 3.1  Insert batch-level row
    ----------------------------------------------------------------
    INSERT INTO core.da_capacity_batch (
        market, delivery_date, delivery_area, unit, total_import, total_export, updated_at, raw
    )
    SELECT  v_body->>'market',
            (v_body->>'deliveryDateCET')::date,
            v_body->>'deliveryArea',
            v_body->>'unit',
            (v_body->>'totalImport')::numeric,
            (v_body->>'totalExport')::numeric,
            (v_body->>'updatedAt')::timestamptz,
            v_body
    RETURNING batch_id INTO v_batch_id;

    ----------------------------------------------------------------
    -- 3.2  Hourly records
    ----------------------------------------------------------------
    INSERT INTO core.da_capacity_hourly
    SELECT  v_batch_id,
            (h->>'deliveryStart')::timestamptz,
            (h->>'deliveryEnd')::timestamptz,
            import.key,							   
            (import.value)::numeric,                   
            export.key,							  
            (export.value)::numeric
      FROM  jsonb_array_elements(v_body->'capacities') AS h,
            LATERAL jsonb_each(h->'importsByConnection')  AS import(key,value),
            LATERAL jsonb_each(h->'exportsByConnection')  AS export(key,value);

    -- 3.3) count how many rows got inserted
    GET DIAGNOSTICS v_count = ROW_COUNT;

    ----------------------------------------------------------------
    -- 3.4) update the request‐log
    ----------------------------------------------------------------
    UPDATE ext.api_request_log
       SET batch_id      = v_batch_id,
           delivery_date = v_delivery_dt,
           record_count  = v_count
     WHERE request_id    = p_request_id;

END;
$$;

CREATE OR REPLACE FUNCTION core.build_da_capacity_url(p_date date)
RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT
AS $$
BEGIN
    RETURN format(
      'https://dataportal-api.nordpoolgroup.com/api/DayAheadCapacities?date=%s&market=N2EX_DayAhead&deliveryArea=NO2',
      to_char(p_date, 'YYYY-MM-DD')
    );
END;
$$;
