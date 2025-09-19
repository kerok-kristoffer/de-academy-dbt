
-- stores
{{ 
    config({ "materialized":'table',
    "transient":true,
    "alias":'STORES_RAW_COPY',
    "pre_hook": macros_copy_walmart_stores_csv('STORES_COPY', 'stores.csv'),
    "database": 'WALMART_DB',
    "schema": 'SILVER'
    })
}}


WITH raw AS(
SELECT 
      store_id::NUMBER           AS store_id,
      store_type::VARCHAR(15)    AS store_type,
      try_to_number(store_size)::NUMBER         AS store_size,
      insert_dts                 AS insert_dts,
      update_dts                 AS update_dts,
      source_file_name           AS source_file_name,
      source_file_row_number     AS source_file_row_number
FROM {{source('walmart_raw','STORES_COPY')}}
)
SELECT *
FROM raw