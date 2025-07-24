{{
    config(
        materialized='view'
    )
}}

SELECT * FROM {{ ref('customer') }}
WHERE COUNTRY = 'USA'