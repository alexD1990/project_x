with source as (

    select * from {{ source('shop', 'payments') }}

),

renamed as (

    select
        payment_id,
        order_id,
        lower(payment_method)   as payment_method,
        amount,
        lower(status)           as payment_status,
        created_at              as payment_created_at
    from source

)

select * from renamed
