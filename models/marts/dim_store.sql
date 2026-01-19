{{ config(materialized='table') }}

with source as (

  select
    store_id,
    store_name,
    store_type,
    channel,
    city,
    state,
    region,
    store_size,
    monthly_sales_potential,
    is_active
  from {{ ref('stg_stores') }}

),

final as (

  select
    -- stable surrogate key derived from natural key
    to_hex(md5(cast(store_id as string))) as store_key,

    -- natural key kept for traceability
    store_id,

    -- attributes
    store_name,
    store_type,
    channel,
    city,
    state,
    region,
    store_size,
    monthly_sales_potential,
    is_active

  from source

)

select *
from final
