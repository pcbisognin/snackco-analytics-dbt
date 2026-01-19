{{ config(materialized='table') }}

with sales as (

  select
    sale_id,
    sale_date,
    store_id,
    product_id,
    units_sold,
    unit_price,
    total_sales_value,
    promotion_flag
  from {{ ref('stg_sales') }}

),

final as (

  select
    -- grain
    s.sale_id,

    -- dimension keys
    d.date_key,
    p.product_key,
    st.store_key,

    -- natural keys (optional but useful for traceability/debugging)
    s.sale_date,
    s.product_id,
    s.store_id,

    -- measures
    s.units_sold,
    s.unit_price,
    s.total_sales_value,
    s.promotion_flag

  from sales s
  left join {{ ref('dim_date') }} d
    on d.date_day = s.sale_date
  left join {{ ref('dim_product') }} p
    on p.product_id = s.product_id
  left join {{ ref('dim_store') }} st
    on st.store_id = s.store_id

)

select *
from final
