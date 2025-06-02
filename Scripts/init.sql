/********************************************************************
  0 .  Prereqs – extensions & schemas
*********************************************************************/
CREATE EXTENSION IF NOT EXISTS pg_net;   -- async HTTP
CREATE EXTENSION IF NOT EXISTS pg_cron;  -- scheduling

CREATE SCHEMA IF NOT EXISTS ext;   -- config / integration
CREATE SCHEMA IF NOT EXISTS core;  -- raw json parse output
CREATE SCHEMA IF NOT EXISTS conform;  -- conformed tables for querying
