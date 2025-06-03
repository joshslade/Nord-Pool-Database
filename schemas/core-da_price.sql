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

CREATE OR REPLACE FUNCTION core.build_da_prices_url(p_date date)
RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT
AS $$
BEGIN
    RETURN format(
      'https://dataportal-api.nordpoolgroup.com/api/DayAheadPrices?date=%s&market=N2EX_DayAhead&currency=GBP&deliveryArea=NO2,UK',
      to_char(p_date, 'YYYY-MM-DD')
    );
END;
$$;

