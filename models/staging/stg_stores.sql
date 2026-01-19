with 

source as (

    select * from {{ source('raw', 'stores') }}

),

renamed as (

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

    from source

)

select * from renamed