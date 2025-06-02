/* ===========================================================
   0   Prerequisite: make sure pg_cron is installed
   =========================================================== */
CREATE EXTENSION IF NOT EXISTS pg_cron;


/* ===========================================================
   1   Unschedule any jobs with the same names (idempotent)
   =========================================================== */
SELECT cron.unschedule(jobid)
  FROM cron.job
 WHERE jobname IN ('fetch_day_ahead', 'process_day_ahead');


/* ===========================================================
   2   Create the 10:00 job (Europe/London)
   =========================================================== */
SELECT cron.schedule(
    job_name      => 'fetch_day_ahead',
    schedule      => '5 9 * * *',
    command       => $$SELECT ext.api_fetch_all()$$
    -- run_as, database default to current; change if you need
);


/* ===========================================================
   3   Create the 10:05 job
   =========================================================== */
SELECT cron.schedule(
    job_name      => 'process_day_ahead',
    schedule      => '10 9 * * * ',
    command       => $$SELECT ext.process_api_responses()$$
);


/* ===========================================================
   4   Verify
   =========================================================== */
SELECT jobid, jobname, schedule
  FROM cron.job
 ORDER BY jobname;