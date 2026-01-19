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

-- step 1: build a "group id" that increments every time we are NOT out of stock
with_groups as (

  select
    j.*,
    sum(case when j.is_out_of_stock = false then 1 else 0 end)
      over (partition by j.store_key, j.product_key order by j.date) as oos_group
  from joined j

),

-- step 2: compute streak length within each group
final as (

  select
    wg.*,
    case
      when wg.is_out_of_stock = false then 0
      else row_number() over (
        partition by wg.store_key, wg.product_key, wg.oos_group
        order by wg.date
      )
    end as oos_streak_days
  from with_groups wg

)

select
  -- keep output tidy (do not expose the helper group)
  * except (oos_group)
from final
order by date, store_id, product_id

