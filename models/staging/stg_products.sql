select
  product_id,
  product_name,
  brand,
  category,
  sub_category,
  flavor,
  package_type,
  cast(package_size_g as int64) as package_size_g,
  cast(unit_price as numeric) as unit_price,
  cast(launch_date as date) as launch_date,
  cast(is_active as bool) as is_active
from {{ source('raw', 'products') }}
