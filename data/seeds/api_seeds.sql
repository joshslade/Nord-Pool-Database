/********************************************************************
  6 .  Seed the config with the Day-Ahead Prices feed
*********************************************************************/

INSERT INTO ext.api_endpoint
        (code, url_builder_fn, date_offset, parser_fn, is_active)
VALUES  ('DA_PRICES',  'core.build_da_prices_url',  1,  'core.parse_da_price',  true)
ON CONFLICT (code) DO
UPDATE SET url_builder_fn = excluded.url_builder_fn,
           date_offset    = excluded.date_offset,
           parser_fn      = excluded.parser_fn,
           is_active      = true;


INSERT INTO ext.api_endpoint
        (code, url_builder_fn, date_offset, parser_fn, is_active)
VALUES  ('DA_INDEX_PRICES',  'core.build_da_idx_prices_url',  1,  'core.parse_da_idx_price',  true)
ON CONFLICT (code) DO
UPDATE SET url_builder_fn = excluded.url_builder_fn,
           date_offset    = excluded.date_offset,
           parser_fn      = excluded.parser_fn,
           is_active      = true;

INSERT INTO ext.api_endpoint
        (code, url_builder_fn, date_offset, parser_fn, is_active)
VALUES  ('DA_CAPACITY',  'core.build_da_capacity_url',  1,  'core.parse_da_capacity',  true)
ON CONFLICT (code) DO
UPDATE SET url_builder_fn = excluded.url_builder_fn,
           date_offset    = excluded.date_offset,
           parser_fn      = excluded.parser_fn,
           is_active      = true;

INSERT INTO ext.api_endpoint
        (code, url_builder_fn, date_offset, parser_fn, is_active)
VALUES  ('DA_FLOW_NO2',  'core.build_da_flow_NO2_url',  1,  'core.parse_da_flow',  true)
ON CONFLICT (code) DO
UPDATE SET url_builder_fn = excluded.url_builder_fn,
           date_offset    = excluded.date_offset,
           parser_fn      = excluded.parser_fn,
           is_active      = true;

INSERT INTO ext.api_endpoint
        (code, url_builder_fn, date_offset, parser_fn, is_active)
VALUES  ('DA_FLOW_UK',  'core.build_da_flow_UK_url',  1,  'core.parse_da_flow',  true)
ON CONFLICT (code) DO
UPDATE SET url_builder_fn = excluded.url_builder_fn,
           date_offset    = excluded.date_offset,
           parser_fn      = excluded.parser_fn,
           is_active      = true;

INSERT INTO ext.api_endpoint
        (code, url_builder_fn, date_offset, parser_fn, is_active)
VALUES  ('DA_VOLUMES',  'core.build_da_vol_url',  1,  'core.parse_da_vol',  true)
ON CONFLICT (code) DO
UPDATE SET url_builder_fn = excluded.url_builder_fn,
           date_offset    = excluded.date_offset,
           parser_fn      = excluded.parser_fn,
           is_active      = true;



select * from ext.api_endpoint ae 


select ext.api_fetch_all()

select ext.process_api_responses()

select * from core.da_vol_hourly

