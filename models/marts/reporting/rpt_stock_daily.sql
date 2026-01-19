{{ config(materialized='table') }}

with base as (

  select
    fs.date_key,
    fs.store_key,
    fs.product_key,
    fs.stock_units,
    fs.out_of_stock_flag,
    fs.replenishment_flag
  from {{ ref('fact_daily_stock') }} fs

),

final as (

  select
    b.date_key,
    d.date_day as date,

    b.store_key,
    s.store_id,
    s.store_name,
    s.channel,
    s.region,
    s.city,
    s.state,

    b.product_key,
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.sub_category,
    p.flavor,
    p.package_type,
    p.package_size_g,

    b.stock_units,
    b.out_of_stock_flag,
    b.replenishment_flag,

    -- convenience flags for BI
    case when b.stock_units = 0 then true else false end as is_out_of_stock

  from base b
  left join {{ ref('dim_date') }} d
    on d.date_key = b.date_key
  left join {{ ref('dim_store') }} s
    on s.store_key = b.store_key
  left join {{ ref('dim_product') }} p
    on p.product_key = b.product_key

)

select *
from final
order by date, store_id, product_id
