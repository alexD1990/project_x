with source as (

    select * from {{ source('shop', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        ordered_at,
        cast(ordered_at as date)                            as order_date,
        cast(date_trunc('MONTH', ordered_at) as date)       as order_month,
        -- Status arrives as 'Completed', 'COMPLETED ', ' completed' ...
        lower(trim(status))                                 as order_status,
        channel,
        upper(currency)                                     as currency,
        coalesce(shipping_amount, 0)                        as shipping_amount,
        nullif(trim(campaign_code), '')                     as campaign_code,
        _loaded_at                                          as loaded_at
    from source

)

select * from renamed
