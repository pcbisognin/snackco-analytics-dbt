{{ config(materialized='table') }}

with stock as (

  select
    date_key,
    date,
    store_key,
    store_id,
    store_name,
    channel,
    region,

    product_key,
    product_id,
    product_name,
    category,

    stock_units,
    is_out_of_stock,
    replenishment_flag
  from {{ ref('rpt_stock_daily') }}

),

sales as (

  select
    date_key,
    store_key,
    product_key,
    units_sold,
    revenue,
    transactions_count
  from {{ ref('rpt_sales_daily') }}

),

joined as (

  select
    s.*,
    coalesce(sa.units_sold, 0) as units_sold,
    coalesce(sa.revenue, 0) as revenue,
    coalesce(sa.transactions_count, 0) as transactions_count,

    case
      when s.is_out_of_stock = true and coalesce(sa.units_sold, 0) = 0 then true
      else false
    end as is_oos_with_zero_sales

  from stock s
  left join sales sa
    on sa.date_key = s.date_key
   and sa.store_key = s.store_key
   and sa.product_key = s.product_key

),

with_streaks as (

  select
    j.*,

    case
      when j.is_out_of_stock = false then 0
      else
        row_number() over (
          partition by j.store_key, j.product_key,
          sum(case when j.is_out_of_stock = false then 1 else 0 end)
            over (partition by j.store_key, j.product_key order by j.date)
          order by j.date
        )
    end as oos_streak_days

  from joined j

)

select *
from with_streaks
order by date, store_id, product_id
