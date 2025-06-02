/********************************************************************
  2 .  Day-Ahead Prices tables  (core.*)
*********************************************************************/

CREATE TABLE IF NOT EXISTS core.da_idx_price_batch (
    batch_id          bigserial PRIMARY KEY,
    market            text,
    delivery_date     date,
    currency          text,
    version           int,
    updated_at        timestamptz,
    raw               jsonb   NOT NULL
);

CREATE TABLE IF NOT EXISTS core.da_idx_price_hourly (
    batch_id       bigint   REFERENCES core.da_idx_price_batch ON DELETE CASCADE,
    delivery_start timestamptz,
    delivery_end   timestamptz,
    area_code      text,
    price_index    numeric,
    PRIMARY KEY (batch_id, delivery_start, area_code)
);


/********************************************************************
  3 .  Parser for this feed only – more can be added later
*********************************************************************/
CREATE OR REPLACE FUNCTION core.parse_da_idx_price(p_request_id bigint)
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
    INSERT INTO core.da_idx_price_batch (
        market, delivery_date, currency,
        version, updated_at, raw
    )
    SELECT  v_body->>'market',
            (v_body->>'deliveryDateCET')::date,
            v_body->>'currency',
            COALESCE((v_body->>'version')::int, 1),
            (v_body->>'updatedAt')::timestamptz,
            v_body
    RETURNING batch_id INTO v_batch_id;

    ----------------------------------------------------------------
    -- 3.2  Hourly records
    ----------------------------------------------------------------
    INSERT INTO core.da_idx_price_hourly
    SELECT  v_batch_id,
            (h->>'deliveryStart')::timestamptz,
            (h->>'deliveryEnd')::timestamptz,
		    area_entry.key,
		    (area_entry.value)::numeric
      FROM jsonb_array_elements(v_body->'multiIndexEntries') AS h,
		   LATERAL (
	       		SELECT * FROM jsonb_each(h->'entryPerArea') 
			) AS area_entry(key, value);


END;
$$;

CREATE OR REPLACE FUNCTION core.build_da_idx_prices_url(p_date date)
RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT
AS $$
BEGIN
    RETURN format(
      'https://dataportal-api.nordpoolgroup.com/api/DayAheadPriceIndices?date=%s&market=N2EX_DayAhead&indexNames=NO2,UK&currency=GBP&resolutionInMinutes=60',
      to_char(p_date, 'YYYY-MM-DD')
    );
END;
$$;


