{{ 
    config({ 
        "materialized":'incremental',
        "unique_key":"date_id",
        "alias":'WALMART_DATE_DIM',
        "database": 'WALMART_DB',
        "schema": 'GOLD',
        "on_schema_change": 'ignore'
    })
}}
 
WITH distinct_days AS(
  -- Distinct date rows from bronze; collapse is_holiday across rows for that day
    SELECT
      report_date::DATE AS report_date,
      MAX(IFF(is_holiday, 1, 0)) = 1 AS is_holiday   -- any true => true
    FROM {{ref('walmart_department')}}
    WHERE report_date IS NOT NULL
    GROUP BY 1
),

prepared AS (
  SELECT
      TO_CHAR(report_date, 'YYYYMMDD')::INT AS date_id,
      report_date                           AS report_date,
      is_holiday                          AS is_holiday,
      CURRENT_TIMESTAMP()                AS insert_date,
      CURRENT_TIMESTAMP()                AS update_date
  FROM distinct_days
),

-- Provide an "existing" CTE only when the target table already exists
{% if is_incremental() %}
existing AS (
  SELECT date_id, insert_date
  FROM {{ this }}
)
{% else %}
existing AS (
  SELECT NULL::int AS date_id, NULL::timestamp AS insert_date
  WHERE FALSE
)
{% endif %},

final_upsert AS (
  SELECT
      p.date_id,
      p.report_date,
      p.is_holiday,
      COALESCE(t.insert_date, p.insert_date) AS insert_date,  -- keep first-seen timestamp
      CURRENT_TIMESTAMP()                     AS update_date  -- refresh on every run/update
  FROM prepared p
  LEFT JOIN existing t
    ON t.date_id = p.date_id
)

SELECT * FROM final_upsert

{% if is_incremental() %}
  -- (Optional) Small optimization: limit to new/changed dates
WHERE report_date > (SELECT COALESCE(MAX(report_date), '1900-01-01') FROM {{ this }})
{% endif %}