/********************************************************************
  0 .  Prereqs – extensions & schemas
*********************************************************************/
CREATE EXTENSION IF NOT EXISTS pg_net;   -- async HTTP
CREATE EXTENSION IF NOT EXISTS pg_cron;  -- scheduling

CREATE SCHEMA IF NOT EXISTS ext;   -- config / integration
CREATE SCHEMA IF NOT EXISTS core;  -- typed analytics tables

/********************************************************************
  1 .  Config & logging objects (ext.*)
*********************************************************************/

-- a) one row per external feed we want to call
CREATE TABLE IF NOT EXISTS ext.api_endpoint (
    endpoint_id    bigserial PRIMARY KEY,
    code           text UNIQUE NOT NULL,              -- e.g. 'DA_PRICES'
    url            text NOT NULL,
    http_headers   jsonb           DEFAULT '{}'::jsonb,
    parser_fn      regproc NOT NULL,                  -- e.g. 'parse_da_price'
    is_active      boolean         DEFAULT true,
    created_at     timestamptz     DEFAULT now()
);

-- b) one row per HTTP request we enqueue
CREATE TABLE IF NOT EXISTS ext.api_request_log (
    request_id     bigint PRIMARY KEY,                  -- pg_net id
    endpoint_id    bigint REFERENCES ext.api_endpoint,
    requested_at   timestamptz     DEFAULT now(),
    processed_at   timestamptz,
    success        boolean,
    note           text
);


/********************************************************************
  2 .  Day-Ahead Prices tables  (core.*)
*********************************************************************/

CREATE TABLE IF NOT EXISTS core.da_price_batch (
    batch_id       bigserial PRIMARY KEY,
    market         text,
    delivery_date  date,
    currency       text,
    exchange_rate  numeric,
    version        int,
    updated_at     timestamptz,
    raw            jsonb   NOT NULL
);

CREATE TABLE IF NOT EXISTS core.da_price_hourly (
    batch_id       bigint      REFERENCES core.da_price_batch ON DELETE CASCADE,
    delivery_start timestamptz,
    delivery_end   timestamptz,
    area_code      text,
    price          numeric,
    PRIMARY KEY (batch_id, delivery_start, area_code)
);



/********************************************************************
  3 .  Parser for this feed only – more can be added later
*********************************************************************/
CREATE OR REPLACE FUNCTION core.parse_da_price(p_request_id bigint)
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
    INSERT INTO core.da_price_batch (
        market, delivery_date, currency, exchange_rate,
        version, updated_at, raw
    )
    SELECT  v_body->>'market',
            (v_body->>'deliveryDateCET')::date,
            v_body->>'currency',
            (v_body->>'exchangeRate')::numeric,
            COALESCE((v_body->>'version')::int, 1),
            (v_body->>'updatedAt')::timestamptz,
            v_body
    RETURNING batch_id INTO v_batch_id;

    ----------------------------------------------------------------
    -- 3.2  Hourly records
    ----------------------------------------------------------------
    INSERT INTO core.da_price_hourly
    SELECT  v_batch_id,
            (h->>'deliveryStart')::timestamptz,
            (h->>'deliveryEnd')::timestamptz,
		    area_entry.key,
		    (area_entry.value)::numeric
      FROM jsonb_array_elements(v_body->'multiAreaEntries') AS h,
		   LATERAL (
	       		SELECT * FROM jsonb_each(h->'entryPerArea') 
			) AS area_entry(key, value);


END;
$$;


/********************************************************************
  4 .  Generic fetcher – queues all active endpoints
*********************************************************************/
CREATE OR REPLACE FUNCTION ext.api_fetch_all()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec  record;
    v_id bigint;
BEGIN
    FOR rec IN
        SELECT * FROM ext.api_endpoint WHERE is_active
    LOOP
        -- enqueue HTTP request with pg_net
        v_id := net.http_get(
                    rec.url,
                    rec.http_headers
                );

        INSERT INTO ext.api_request_log(request_id, endpoint_id)
        VALUES (v_id, rec.endpoint_id);
    END LOOP;
END;
$$;


/********************************************************************
  5 .  Generic processor – handles completed responses
*********************************************************************/
CREATE OR REPLACE FUNCTION ext.process_api_responses()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec  record;
BEGIN
    FOR rec IN
        SELECT  r.request_id,
                r.endpoint_id,
                e.parser_fn
        FROM    ext.api_request_log r
        JOIN    ext.api_endpoint   e USING (endpoint_id)
        WHERE   r.processed_at IS NULL
          AND   EXISTS (SELECT 1
                        FROM net._http_response h
                        WHERE h.id = r.request_id
                          AND h.status_code BETWEEN 200 AND 299)
    LOOP
        BEGIN
            -- 5.1  call the parser linked to that endpoint
            EXECUTE format('SELECT %s($1)', rec.parser_fn)
            USING rec.request_id;


            -- 5.2  mark as done
            UPDATE ext.api_request_log
               SET processed_at = now(),
                   success      = true
             WHERE request_id   = rec.request_id;

        EXCEPTION WHEN others THEN
            UPDATE ext.api_request_log
               SET processed_at = now(),
                   success      = false,
                   note         = SQLERRM
             WHERE request_id   = rec.request_id;
        END;
    END LOOP;
END;
$$;


/********************************************************************
  6 .  Seed the config with the Day-Ahead Prices feed
*********************************************************************/
INSERT INTO ext.api_endpoint(code, url, parser_fn)
VALUES ('DA_PRICES',
        'https://dataportal-api.nordpoolgroup.com/api/DayAheadPrices?date=2025-05-31&market=N2EX_DayAhead&currency=GBP&deliveryArea=NO2,UK',
        'core.parse_da_price')
ON CONFLICT (code) DO NOTHING;