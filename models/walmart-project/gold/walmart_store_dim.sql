{{ 
        config({ 
        "materialized":'incremental',
        "unique_key":['store_id','dept_id'],
        "alias":'WALMART_STORE_DIM',
        "database": 'WALMART_DB',
        "schema": 'GOLD',
        "on_schema_change": 'ignore'
    }) }}

-- 1) Department rows: choose the *latest* record per (store_id, dept_id)
with dept_src as (
  select
    store_id::number        as store_id,
    department_id::number   as dept_id,
    report_date::date       as report_date
  from {{ ref('walmart_department') }}
  where store_id is not null and department_id is not null and report_date is not null
),
latest_dept as (
  select store_id, dept_id, report_date
  from (
    select
      d.*,
      row_number() over (partition by store_id, dept_id order by report_date desc) as rn
    from dept_src d
  )
  where rn = 1
),

-- 2) Store rows: dedupe to one row per store_id (pick latest by update_dts, then insert_dts)
store_src as (
  select
    store_id::number       as store_id,
    store_type::varchar(15) as store_type,
    store_size::number     as store_size,
    update_dts, insert_dts
  from {{ ref('walmart_stores') }}
),
latest_store as (
  select store_id, store_type, store_size
  from (
    select
      s.*,
      row_number() over (
        partition by store_id
        order by update_dts desc nulls last, insert_dts desc nulls last
      ) as rn
    from store_src s
  )
  where rn = 1
),

-- 3) Join to form the dimension grain (store_id, dept_id)
prepared as (
  select
    d.store_id,
    d.dept_id,
    s.store_type,
    s.store_size,
    current_timestamp() as insert_date,
    current_timestamp() as update_date
  from latest_dept d
  left join latest_store s
    on s.store_id = d.store_id
),

-- 4) Preserve first-seen insert_date on upsert (SCD1)
{% if is_incremental() %}
existing as (
  select store_id, dept_id, insert_date
  from {{ this }}
)
{% else %}
existing as (
  select null::number as store_id, null::number as dept_id, null::timestamp as insert_date
  where false
)
{% endif %}

select
  p.store_id,
  p.dept_id,
  p.store_type,
  p.store_size,
  coalesce(e.insert_date, p.insert_date) as insert_date,  -- keep first-seen timestamp
  current_timestamp()                    as update_date   -- refresh on each upsert
from prepared p
left join existing e
  on e.store_id = p.store_id and e.dept_id = p.dept_id

{% if is_incremental() %}
-- optional small optimization if report_date strictly increases in dept data:
 where p.store_id || '-' || p.dept_id in (
   select distinct store_id || '-' || dept_id
   from {{ ref('walmart_raw_import') }}
   where report_date > (select coalesce(max(update_date)::date, '1900-01-01') from {{ this }})
)
{% endif %}
