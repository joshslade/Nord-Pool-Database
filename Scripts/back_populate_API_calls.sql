/********************************************************************
   	Back-populate data with batch generated API calls
*********************************************************************/

DO $$
DECLARE
    ep RECORD;
    dt DATE;
    v_url TEXT;
	v_req_id bigint;
BEGIN
    FOR ep IN
        SELECT * FROM ext.api_endpoint where code in ('DA_VOLUMES')
    LOOP
        FOR dt IN
            SELECT generate_series('2025-03-29'::date, '2025-06-02', '1 day')
        LOOP
            EXECUTE format('SELECT %s($1)', ep.url_builder_fn)
            INTO v_url
            USING dt::date;


	        v_req_id := net.http_get(
	                       v_url,
	                       ep.http_headers
	                   );
			INSERT INTO ext.api_request_log(request_id, endpoint_id, url)
        	VALUES (v_req_id, ep.endpoint_id, v_url);

            -- You could also INSERT result into a log table here instead of RAISE NOTICE

        END LOOP;
    END LOOP;
END $$;

select * from ext.api_request_log where success is null

select ext.process_api_responses()

select * from core.da_vol_batch

select 
arl.endpoint_id,
hr.* 
from net._http_response hr 
join ext.api_request_log arl on arl.request_id = hr.id
where arl.success is null
