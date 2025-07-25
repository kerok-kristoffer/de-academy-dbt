{{ 
    config
    ( 
        materialized='table'
    )
}}

select {{ concat_macro('John','Smith') }} as name
