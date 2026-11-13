-- Orders with the monthly exchange rate to EUR attached.

with orders as (

    select * from {{ ref('stg_shop__orders') }}

),

currency_rates as (

    select * from {{ ref('currency_rates') }}

),

joined as (

    select
        orders.*,
        currency_rates.rate_to_eur,
        cast(orders.shipping_amount * currency_rates.rate_to_eur as decimal(12, 2))    as shipping_amount_eur,
        orders.order_status in ('completed', 'shipped')                                 as is_revenue_order
    from orders
    left join currency_rates
        on  orders.currency = currency_rates.currency
        and orders.order_month = currency_rates.rate_month

)

select * from joined
