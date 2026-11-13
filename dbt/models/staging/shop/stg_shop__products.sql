with source as (

    select * from {{ source('shop', 'products') }}

),

renamed as (

    select
        product_sku,
        product_name,
        upper(category_code)    as category_code,
        brand,
        list_price_eur,
        cost_price_eur,
        is_active,
        launched_at
    from source

)

select * from renamed
