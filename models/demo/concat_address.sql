
{{ 
    config
    ( 
        materialized='table'
    )
}}

select {{ concat_macro('123street','Chicago') }} as address