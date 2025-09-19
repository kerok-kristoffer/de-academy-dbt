
--walmart_department
{{ 
    config({ 
    "materialized":'table',
    "transient":true,
    "alias":'WALMART_RAW_IMPORT',
    "unique_key":['store_id', 'department_id'],
    "pre_hook": macros_copy_walmart_csv('DEPARTMENT_COPY', 'department.csv'),
    "database": 'WALMART_DB',
    "schema": 'SILVER'
    })
}}
 
WITH transform AS(
SELECT 
      store_id::NUMBER           AS store_id,
      department_id::NUMBER      AS department_id,
      report_date::DATE          AS report_date,
      try_to_number(weekly_sales)::NUMBER(18,2) AS weekly_sales,
      try_to_boolean(is_holiday)::BOOLEAN        AS is_holiday,
      insert_dts                 AS insert_dts,
      update_dts                 AS update_dts,
      source_file_name           AS source_file_name,
      source_file_row_number     AS source_file_row_number
FROM {{source('walmart_raw','DEPARTMENT_COPY')}}
)
SELECT *
FROM transform