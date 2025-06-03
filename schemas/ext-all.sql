/********************************************************************
  1 .  Config & logging objects (ext.*)
*********************************************************************/

-- a) one row per external feed we want to call
CREATE TABLE IF NOT EXISTS ext.api_endpoint (
    endpoint_id     bigserial PRIMARY KEY,
    code            text       UNIQUE NOT NULL,        -- short handle e.g. 'DA_PRICES'
    url_template    text,                              -- optional static URL
    url_builder_fn  regproc,                           -- optional dynamic builder
    http_headers    jsonb      DEFAULT '{}'::jsonb,
    date_offset     int        DEFAULT 0,              -- relative to “today”
    parser_fn       regproc    NOT NULL,
    is_active       boolean    DEFAULT true,
    created_at      timestamptz DEFAULT now()
);


-- b) one row per HTTP request we enqueue
CREATE TABLE IF NOT EXISTS ext.api_request_log (
    request_id     bigint PRIMARY KEY,                  -- pg_net id
    endpoint_id    bigint REFERENCES ext.api_endpoint,
    requested_at   timestamptz     DEFAULT now(),
    processed_at   timestamptz,
    success        boolean,
    url			   text,
    note           text
);

ALTER TABLE ext.api_request_log
  ADD COLUMN batch_id      bigint,
  ADD COLUMN delivery_date date,
  ADD COLUMN record_count  integer;


CREATE TABLE IF NOT EXISTS ext.api_daily_alerts (
  alert_id    bigserial PRIMARY KEY,
  alert_date  date       NOT NULL,         -- usually current_date
  issues      text[]    NOT NULL,         -- array of “endpoint_id: description”
  created_at  timestamptz DEFAULT now()
);


/********************************************************************
  4 .  Generic fetcher – queues all active endpoints
*********************************************************************/
CREATE OR REPLACE FUNCTION ext.api_fetch_all()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    rec        record;
    v_url      text;
    v_req_id   bigint;
    v_today    date := (CURRENT_DATE AT TIME ZONE 'Europe/London');  -- local “today”
BEGIN
    FOR rec IN
        SELECT * FROM ext.api_endpoint
        WHERE is_active
    LOOP
        /* 1. determine which date we want */
        IF rec.date_offset IS NOT NULL THEN
            v_url := NULL;  -- init
            /* 2. build URL dynamically if builder function exists */
            IF rec.url_builder_fn IS NOT NULL THEN
                EXECUTE format('SELECT %s($1)', rec.url_builder_fn)
                INTO  v_url
                USING (v_today + (rec.date_offset||' days')::interval)::date;
            ELSE
                /* 3. fall back to static template */
                v_url := rec.url_template;
            END IF;
        END IF;

        /* 4. enqueue */
        v_req_id := net.http_get(
                       v_url,
                       rec.http_headers
                   );

        INSERT INTO ext.api_request_log(request_id, endpoint_id, url)
        VALUES (v_req_id, rec.endpoint_id, v_url);
    END LOOP;
END;
$$;

select ext.api_fetch_all()

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
                          AND h.status_code = 200)
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

CREATE OR REPLACE FUNCTION ext.check_daily_api_status()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_total_logged     int;
  v_total_processed  int;
  v_total_success    int;
  v_total_inserts    int;
  v_problems         text[];
BEGIN
  WITH today_requests AS (
    SELECT
      endpoint_id,
      processed_at,
      success,
	  record_count,
      note
    FROM ext.api_request_log
    WHERE requested_at::date = current_date
  )
  , summary AS (
    SELECT 
      COUNT(*)                                       AS total_logged,
      COUNT(*) FILTER (WHERE processed_at IS NOT NULL)   AS total_processed,
      COUNT(*) FILTER (WHERE success = true)         AS total_success,
      COUNT(*) FILTER (WHERE record_count % 25 = 0)         AS total_successful_inserts,	
      ARRAY_AGG(
        CASE 
          WHEN processed_at IS NULL 
            THEN endpoint_id::text || ': never processed'
          WHEN success = false 
            THEN endpoint_id::text 
                 || ': parser‐error ('
                 || COALESCE(note, 'no detail') || ')'
		  WHEN record_count % 25 != 0
          	THEN endpoint_id::text 
                 || ': hourly-insert-error ('
                 || COALESCE(note, 'no detail') || ')'
          ELSE NULL
        END
      ) FILTER (WHERE processed_at IS NULL OR success = false OR record_count % 25 != 0) AS problems
    FROM today_requests
  )
  SELECT total_logged, total_processed, total_success, total_successful_inserts, problems
    INTO v_total_logged, v_total_processed, v_total_success, v_total_inserts, v_problems
  FROM summary;

  -- Now decide if we need to insert an alert
  IF v_total_logged  < 6 
     OR v_total_processed < 6 
     OR v_total_success < 6 
     OR v_total_inserts < 6 THEN

    INSERT INTO ext.api_daily_alerts(alert_date, issues)
    VALUES (current_date, v_problems);

    -- (Optionally, RAISE NOTICE so that a cron‐wrapper can grep for it.)
    RAISE NOTICE 'API‐daily alert (date=%, logged=% total, processed=% total, success=% total, inserts=% total): %',
                 current_date, v_total_logged, v_total_processed, v_total_success, v_total_inserts,
                 v_problems;
  ELSE
  -- If all six succeeded (logged=6, processed=6, success=6), do nothing.
	RAISE NOTICE 'API-daily alert (date=%): All checks passed', current_date	;
  END IF;

END;
$$;

SELECT ext.check_daily_api_status();

select * from ext.api_daily_alerts