with 

source as (

    select * from {{ source('raw', 'stock') }}

),

renamed as (

    select
        stock_id,
        stock_date,
        store_id,
        product_id,
        stock_units,
        out_of_stock_flag,
        replenishment_flag

    from source

)

select * from renamed