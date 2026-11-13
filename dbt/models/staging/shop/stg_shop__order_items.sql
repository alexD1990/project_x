with source as (

    select * from {{ source('shop', 'order_items') }}

),

renamed as (

    select
        order_item_id,
        order_id,
        product_sku,
        quantity,
        unit_price,
        discount_pct,
        cast(quantity * unit_price as decimal(12, 2))                       as gross_amount,
        cast(quantity * unit_price * discount_pct as decimal(12, 2))        as discount_amount,
        cast(quantity * unit_price * (1 - discount_pct) as decimal(12, 2))  as net_amount
    from source

)

select * from renamed
