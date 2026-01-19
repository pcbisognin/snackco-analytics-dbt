{{ config(materialized='table') }}

with base as (

  select
    fs.date_key,
    fs.store_key,
    fs.product_key,
    fs.units_sold,
    fs.total_sales_value,
    fs.promotion_flag
  from {{ ref('fact_sale') }} fs

),

agg as (

  select
    date_key,
    store_key,
    product_key,

    count(*) as transactions_count,
    sum(units_sold) as units_sold,
    sum(total_sales_value) as revenue,

    sum(case when promotion_flag then total_sales_value else 0 end) as promo_revenue,
    sum(case when promotion_flag then units_sold else 0 end) as promo_units_sold

  from base
  group by 1, 2, 3

),

final as (

  select
    a.date_key,
    d.date_day as date,

    a.store_key,
    s.store_id,
    s.store_name,
    s.channel,
    s.region,
    s.city,
    s.state,

    a.product_key,
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    p.sub_category,
    p.flavor,
    p.package_type,
    p.package_size_g,

    a.transactions_count,
    a.units_sold,
    a.revenue,
    a.promo_units_sold,
    a.promo_revenue

  from agg a
  left join {{ ref('dim_date') }} d
    on d.date_key = a.date_key
  left join {{ ref('dim_store') }} s
    on s.store_key = a.store_key
  left join {{ ref('dim_product') }} p
    on p.product_key = a.product_key

)

select *
from final
order by date, store_id, product_id
