{{ config(materialized='table') }}

with stock as (

  select
    stock_id,
    stock_date,
    store_id,
    product_id,
    stock_units,
    out_of_stock_flag,
    replenishment_flag
  from {{ ref('stg_stock') }}

),

final as (

  select
    -- grain identifier (optional, but useful)
    s.stock_id,

    -- dimension keys
    d.date_key,
    p.product_key,
    st.store_key,

    -- natural keys (debug / traceability)
    s.stock_date,
    s.product_id,
    s.store_id,

    -- measures / indicators
    s.stock_units,
    s.out_of_stock_flag,
    s.replenishment_flag

  from stock s
  left join {{ ref('dim_date') }} d
    on d.date_day = s.stock_date
  left join {{ ref('dim_product') }} p
    on p.product_id = s.product_id
  left join {{ ref('dim_store') }} st
    on st.store_id = s.store_id

)

select *
from final
