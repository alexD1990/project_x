-- One row per order: what was paid, refunded and how many attempts failed.

with payments as (

    select * from {{ ref('stg_shop__payments') }}

),

per_order as (

    select
        order_id,
        sum(case when payment_status = 'success'  then amount else 0 end)   as paid_amount,
        sum(case when payment_status = 'refunded' then amount else 0 end)   as refunded_amount,
        count_if(payment_status = 'failed')                                 as failed_attempts,
        min(payment_created_at) filter (where payment_status = 'success')  as paid_at,
        max_by(payment_method, payment_created_at)
            filter (where payment_status = 'success')                       as payment_method
    from payments
    group by order_id

)

select * from per_order
