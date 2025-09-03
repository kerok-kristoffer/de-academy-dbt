{% macro macros_copy_walmart_fact_csv(table_nm, file_nm) %} 

TRUNCATE TABLE WALMART_DB.BRONZE.{{ table_nm }};
 
COPY INTO WALMART_DB.BRONZE.{{ table_nm }}
FROM 
(
SELECT
    $1    AS store_id,
    $2    AS report_date,
    $3    AS temperature,
    $4    AS fuel_price,
    $5    AS markdown_1,
    $6    AS markdown_2,
    $7    AS markdown_3,
    $8    AS markdown_4,
    $9    AS markdown_5,
    $10    AS cpi,
    $11    AS unemployment,
    $12    AS is_holiday,
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

 