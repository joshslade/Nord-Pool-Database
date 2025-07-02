WITH today_requests AS (
    SELECT
      endpoint_id,
      processed_at,
      success,
      delivery_date,
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
      COUNT(*) FILTER (WHERE record_count % 24 = 0)         AS total_successful_inserts,
      ARRAY_AGG(
        CASE 
          WHEN processed_at IS NULL 
            THEN endpoint_id::text || ': never processed'
          WHEN success = false 
            THEN endpoint_id::text 
                 || ': parser‐error ('
                 || COALESCE(note, 'no detail') || ')'
          when record_count % 24 != 0
          	THEN endpoint_id::text 
                 || ': hourly-insert-error ('
                 || COALESCE(note, 'no detail') || ')'
          ELSE NULL
        END
      ) FILTER (WHERE processed_at IS NULL OR success = false or record_count % 24 != 0) AS problems
    FROM today_requests
  )
  SELECT total_logged, total_processed, total_success, total_successful_inserts, problems
  FROM summary;