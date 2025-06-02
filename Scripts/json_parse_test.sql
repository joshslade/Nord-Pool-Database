/* Batch-level row that would go into core.da_idx_price_batch */
WITH src AS (
    SELECT
        $json$
{
    "version": 2,
    "auctionIdentifier": null,
    "dataSource": "Auction",
    "market": "N2EX_DayAhead",
    "unit": "MW",
    "totalVolumeUnit": "MWh",
    "blockVolumesUnit": "MWh",
    "areaSummaries": {
        "totalPerArea": {
            "NO2": {
                "buy": 9745.2,
                "sell": 30658.8
            },
            "UK": {
                "buy": 306098.8,
                "sell": 286068.8
            }
        },
        "deliveryStart": "2025-05-30",
        "deliveryEnd": "2025-05-30",
        "averagePerArea": {
            "NO2": {
                "buy": 406.05,
                "sell": 1277.45
            },
            "UK": {
                "buy": 12754.117,
                "sell": 11919.533
            }
        },
        "maxPerArea": {
            "NO2": {
                "buy": 1601.1,
                "sell": 1911.0
            },
            "UK": {
                "buy": 16006.3,
                "sell": 14606.6
            }
        },
        "minPerArea": {
            "NO2": {
                "buy": 0.0,
                "sell": 535.9
            },
            "UK": {
                "buy": 11316.8,
                "sell": 10051.7
            }
        }
    },
    "blockVolumeAggregates": [
        {
            "blockName": "Base",
            "deliveryStart": "2025-05-29T22:00:00Z",
            "deliveryEnd": "2025-05-30T22:00:00Z",
            "volumePerArea": {
                "NO2": {
                    "buy": 9745.2,
                    "sell": 30658.8
                },
                "UK": {
                    "buy": 306098.8,
                    "sell": 286068.8
                }
            }
        },
        {
            "blockName": "Overnight",
            "deliveryStart": "2025-05-29T22:00:00Z",
            "deliveryEnd": "2025-05-30T06:00:00Z",
            "volumePerArea": {
                "NO2": {
                    "buy": 6912.4,
                    "sell": 6037.0
                },
                "UK": {
                    "buy": 94004.4,
                    "sell": 95022.1
                }
            }
        },
        {
            "blockName": "Extended peak",
            "deliveryStart": "2025-05-30T06:00:00Z",
            "deliveryEnd": "2025-05-30T22:00:00Z",
            "volumePerArea": {
                "NO2": {
                    "buy": 2832.8,
                    "sell": 24621.8
                },
                "UK": {
                    "buy": 212094.4,
                    "sell": 191046.7
                }
            }
        },
        {
            "blockName": "Peak",
            "deliveryStart": "2025-05-30T06:00:00Z",
            "deliveryEnd": "2025-05-30T18:00:00Z",
            "volumePerArea": {
                "NO2": {
                    "buy": 2789.9,
                    "sell": 18782.9
                },
                "UK": {
                    "buy": 150659.4,
                    "sell": 135210.5
                }
            }
        },
        {
            "blockName": "Block 3+4",
            "deliveryStart": "2025-05-30T06:00:00Z",
            "deliveryEnd": "2025-05-30T14:00:00Z",
            "volumePerArea": {
                "NO2": {
                    "buy": 2308.9,
                    "sell": 12505.9
                },
                "UK": {
                    "buy": 97640.7,
                    "sell": 87790.6
                }
            }
        },
        {
            "blockName": "Block 5",
            "deliveryStart": "2025-05-30T14:00:00Z",
            "deliveryEnd": "2025-05-30T18:00:00Z",
            "volumePerArea": {
                "NO2": {
                    "buy": 481.0,
                    "sell": 6277.0
                },
                "UK": {
                    "buy": 53018.7,
                    "sell": 47419.9
                }
            }
        },
        {
            "blockName": "Block 6",
            "deliveryStart": "2025-05-30T18:00:00Z",
            "deliveryEnd": "2025-05-30T22:00:00Z",
            "volumePerArea": {
                "NO2": {
                    "buy": 42.9,
                    "sell": 5838.9
                },
                "UK": {
                    "buy": 61435.0,
                    "sell": 55836.2
                }
            }
        }
    ],
    "areaStates": [
        {
            "state": "Final",
            "areas": [
                "NO2",
                "UK"
            ]
        }
    ],
    "updatedAt": "2025-05-29T08:57:12.3135591Z",
    "deliveryDateCET": "2025-05-30",
    "areas": [
        "NO2",
        "UK"
    ],
    "multiAreaEntries": [
        {
            "deliveryStart": "2025-05-29T22:00:00Z",
            "deliveryEnd": "2025-05-29T23:00:00Z",
            "entryPerArea": {
                "NO2": {
                    "buy": 0.0,
                    "sell": 984.0
                },
                "UK": {
                    "buy": 12573.4,
                    "sell": 11622.9
                }
            }
        },
        {
            "deliveryStart": "2025-05-29T23:00:00Z",
            "deliveryEnd": "2025-05-30T00:00:00Z",
            "entryPerArea": {
                "NO2": {
                    "buy": 440.3,
                    "sell": 959.3
                },
                "UK": {
                    "buy": 11852.0,
                    "sell": 11350.6
                }
            }
        }
    ]
}
$json$::jsonb AS j
)
SELECT
    j->>'market'                      AS market,
    (j->>'deliveryDateCET')::date     AS delivery_date,
    (j->>'deliveryArea')::date        AS delivery_area,
    j->>'unit'                        AS unit,
    (j->>'updatedAt')::timestamptz    AS updated_at,
    (j->>'totlaImport')::numeric      AS total_import,
    (j->>'totlaExport')::numeric      AS total_export,
    j                                 AS raw         -- full JSON if you want it
FROM src;


/* Hourly rows that would go into core.da_idx_price_hourly */
WITH src AS (
    SELECT
        $json$
{
    "version": 2,
    "auctionIdentifier": null,
    "dataSource": "Auction",
    "market": "N2EX_DayAhead",
    "unit": "MW",
    "totalVolumeUnit": "MWh",
    "blockVolumesUnit": "MWh",
    "areaStates": [
        {
            "state": "Final",
            "areas": [
                "NO2",
                "UK"
            ]
        }
    ],
    "updatedAt": "2025-05-29T08:57:12.3135591Z",
    "deliveryDateCET": "2025-05-30",
    "areas": [
        "NO2",
        "UK"
    ],
    "multiAreaEntries": [
        {
            "deliveryStart": "2025-05-29T22:00:00Z",
            "deliveryEnd": "2025-05-29T23:00:00Z",
            "entryPerArea": {
                "NO2": {
                    "buy": 0.0,
                    "sell": 984.0
                },
                "UK": {
                    "buy": 12573.4,
                    "sell": 11622.9
                }
            }
        },
        {
            "deliveryStart": "2025-05-29T23:00:00Z",
            "deliveryEnd": "2025-05-30T00:00:00Z",
            "entryPerArea": {
                "NO2": {
                    "buy": 440.3,
                    "sell": 959.3
                },
                "UK": {
                    "buy": 11852.0,
                    "sell": 11350.6
                }
            }
        }
    ]
}
        $json$::jsonb AS j
)
SELECT
    (h->>'deliveryStart')::timestamptz AS delivery_start,
    (h->>'deliveryEnd')::timestamptz   AS delivery_end,
    a.key							   as connection_area,
    (a.value)->>'buy'                  AS buy_mwh,
    (a.value)->>'sell'                 AS sell_mwh,
    h AS raw
FROM src,
     jsonb_array_elements(j->'multiAreaEntries') AS h,
     LATERAL jsonb_each(h->'entryPerArea')  AS a(key,value);







/*
a.key                              AS area_code,
    (a.value)::numeric                 AS price_index
    */