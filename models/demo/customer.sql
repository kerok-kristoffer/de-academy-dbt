
{{
    config(
        materialized='table'
    )
}}



WITH customer_src as
(
    SELECT CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    COUNTRY,
    CREATED_AT,
    CURRENT_TIMESTAMP as INSERT_DTS
    FROM {{ source('customer', 'CUSTOMER_SRC') }}
)

SELECT * FROM customer_src