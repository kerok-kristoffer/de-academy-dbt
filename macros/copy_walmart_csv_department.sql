{% macro macros_copy_walmart_csv(table_nm, file_nm) %} 

TRUNCATE TABLE WALMART_DB.BRONZE.{{ table_nm }};
 
COPY INTO WALMART_DB.BRONZE.{{ table_nm }}
FROM 
(
SELECT
    $1 AS store_id,
    $2 AS department_id,
    $3 AS report_date,
    $4 AS weekly_sales,
    $5 AS is_holiday,
    CURRENT_TIMESTAMP() AS insert_dts,
    CURRENT_TIMESTAMP() AS update_dts,
    metadata$filename AS source_file_name,
    metadata$file_row_number AS source_file_row_number
FROM @WALMART_DB.BRONZE.WALMART_RAW_STAGE/{{ file_nm }}
)
FILE_FORMAT = WALMART_DB.BRONZE.MY_CSV_FORMAT
PURGE=FALSE
FORCE = TRUE
;
 
{% endmacro %}

 