{{ 
    config({ "materialized":'table',
    "transient":true,
    "alias":'FACT_RAW_COPY',
    "pre_hook": macros_copy_walmart_fact_csv('FACT_COPY', 'fact.csv'),
    "database": 'WALMART_DB',
    "schema": 'BRONZE'
    })
}}


WITH raw AS(
SELECT 
      store_id::NUMBER              AS store_id,
      report_date::DATE             AS report_date,
      temperature::NUMBER           AS temperature,
      fuel_price::NUMBER            AS fuel_price,
      markdown_1                    AS markdown_1,
      markdown_2                    AS markdown_2,
      markdown_3                    AS markdown_3,
      markdown_4                    AS markdown_4,
      markdown_5                    AS markdown_5,
      cpi                           AS cpi,
      unemployment                  AS unemployment,
      is_holiday                    AS is_holiday,
      update_dts                    AS update_dts,
      source_file_name              AS source_file_name,
      source_file_row_number        AS source_file_row_number
FROM {{source('walmart_raw','FACT_COPY')}}
)
SELECT *
FROM raw