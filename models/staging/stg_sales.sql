with 

source as (

    select * from {{ source('raw', 'sales') }}

),

renamed as (

    select
        sale_id,
        sale_date,
        store_id,
        product_id,
        units_sold,
        unit_price,
        total_sales_value,
        promotion_flag

    from source

)

select * from renamed