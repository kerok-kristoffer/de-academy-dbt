
{{ 
    config({ 
        "materialized":'incremental',
        "alias":'WALMART_WEEKLY_REPORTS_FACT',
        "database": 'WALMART_DB',
        "schema": 'GOLD',
        "unique_key":['date_id','store_id','dept_id', 'vrsn_start_date'],
        "on_schema_change": 'ignore',
        "incremental_strategy":'append'
    })
}}

--    de-dupe to one row per BK input first (latest record per BK).
WITH dept AS (
  SELECT
    store_id::number      as store_id,
    department_id::number as dept_id,
    report_date::date     as report_date,
    try_to_decimal(weekly_sales, 18, 2)::number as weekly_sales
  from {{ ref('walmart_department') }}
),
dept_latest AS (
  SELECT store_id, dept_id, report_date, weekly_sales
  FROM (
    SELECT
      d.store_id,
      d.dept_id,
      d.report_date,
      d.weekly_sales,
      row_number() OVER (
        PARTITION BY report_date, store_id, dept_id
        ORDER BY report_date DESC
      ) AS rn
    from dept d
  )
  where rn = 1
),

facts AS (
      select
    store_id::number  as store_id,
    report_date::date as report_date,
    try_to_number(temperature)  as temperature,
    try_to_number(fuel_price)   as fuel_price,
    try_to_number(cpi)          as cpi,
    try_to_number(unemployment) as unemployment,
    try_to_number(markdown_1)   as markdown1,
    try_to_number(markdown_2)   as markdown2,
    try_to_number(markdown_3)   as markdown3,
    try_to_number(markdown_4)   as markdown4,
    try_to_number(markdown_5)   as markdown5
  from {{ ref('walmart_fact') }}
),


-- build base from date, store, fact.
  base AS (
SELECT
    dd.date_id, s.store_id, d.dept_id AS dept_id, s.store_size,
    d.weekly_sales AS store_weekly_sales,
    f.fuel_price, f.temperature AS store_temperature,
    f.unemployment, f.cpi, 
    f.markdown1, f.markdown2, f.markdown3, f.markdown4, f.markdown5
FROM dept_latest d
LEFT JOIN {{ ref('walmart_stores') }} s
    ON d.store_id = s.store_id
LEFT JOIN facts f
    ON d.store_id = f.store_id 
    AND d.report_date = f.report_date
LEFT JOIN {{ ref('walmart_date_dim')}} dd
    ON d.report_date = dd.report_date
WHERE dd.date_id is not null
    AND s.store_id is not null
    AND d.dept_id  is not null
),


--  Compute the row change hash & version start
prepared as (
  select
    *,
    md5(concat_ws('||',
      coalesce(to_varchar(store_size), ''),
      coalesce(to_varchar(store_weekly_sales), ''),
      coalesce(to_varchar(fuel_price), ''),
      coalesce(to_varchar(store_temperature), ''),
      coalesce(to_varchar(unemployment), ''),
      coalesce(to_varchar(cpi), ''),
      coalesce(to_varchar(markdown1), ''),
      coalesce(to_varchar(markdown2), ''),
      coalesce(to_varchar(markdown3), ''),
      coalesce(to_varchar(markdown4), ''),
      coalesce(to_varchar(markdown5), '')
    )) as row_hash,
    current_timestamp() as vrsn_start_date
  from base
),

-- get current open versions of rows to check potential replacements against
    -- only if table already exists
{% if is_incremental() %}
  current_open AS (
  SELECT
    date_id, store_id, dept_id, row_hash
  FROM {{ this }}
  WHERE vrsn_end_date IS NULL          -- returns open entries only
  ),
{% else %}
  current_open AS (
  SELECT null::number as date_id, null::number as store_id, null::number as dept_id, null::string as row_hash
  where false -- returns an empty set
  ),
{% endif %}


-- identify changed rows by comparing hashes on rows with identical unique keys ('date_id','store_id','dept_id')
changed AS (
    SELECT b.* FROM prepared b
    LEFT JOIN current_open t
        ON b.date_id = t.date_id
        AND b.store_id = t.store_id
        AND b.dept_id = t.dept_id
    WHERE t.row_hash IS NULL -- new row
        OR b.row_hash <> t.row_hash -- changed values
)


-- set replaced open rows to closed, ie set vrsn_end_date
{% if is_incremental() %}
    {% call statement('close_currents', fetch_result=False) %}
    UPDATE {{ this }} AS tgt -- alias to enable use of this as table
       SET vrsn_end_date = CURRENT_TIMESTAMP() -- sets end date for the version of the closing row
     WHERE vrsn_end_date is null -- double checking that we only set it on open rows, ie end_date = null
       AND EXISTS ( -- and uniqueIDs are found in changed -- look up EXISTS keyword
         SELECT 1
         FROM changed c
         WHERE c.date_id  = tgt.date_id
           AND c.store_id = tgt.store_id
           AND c.dept_id  = tgt.dept_id
       )
  {% endcall %}

{% endif %}


-- model insert new open rows
SELECT date_id,
    store_id,
    dept_id,
    store_size,
    store_weekly_sales,
    fuel_price,
    store_temperature,
    unemployment,
    cpi,
    markdown1, markdown2, markdown3, markdown4, markdown5,
    vrsn_start_date,
    CAST(null as TIMESTAMP) AS vrsn_end_date,
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date,
    row_hash
FROM changed 


