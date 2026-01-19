{{ config(materialized='table') }}

with source as (

  select
    product_id,
    product_name,
    brand,
    category,
    sub_category,
    flavor,
    package_type,
    package_size_g,
    unit_price,
    launch_date,
    is_active
  from {{ ref('stg_products') }}

),

final as (

  select
    -- surrogate key (stable). Great for joins and future SCD evolution.
    to_hex(md5(cast(product_id as string))) as product_key,

    -- natural key (kept for traceability)
    product_id,

    -- attributes
    product_name,
    brand,
    category,
    sub_category,
    flavor,
    package_type,
    package_size_g,
    unit_price,
    launch_date,
    is_active

  from source

)

select *
from final
