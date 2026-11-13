{{ config(materialized='table') }}

-- One row per order: amounts in the order currency and in EUR, payment state and region.

with orders as (

    select * from {{ ref('int_orders__converted') }}

),

order_items as (

    select * from {{ ref('int_order_items__enriched') }}

),

payments as (

    select * from {{ ref('int_payments__per_order') }}

),

customers as (

    select * from {{ ref('stg_shop__customers') }}

),

country_regions as (

    select * from {{ ref('country_regions') }}

),

items_per_order as (

    select
        order_id,
        count(*)                    as order_lines,
        sum(quantity)               as units,
        sum(net_amount)             as items_net_amount,
        sum(gross_amount_eur)       as items_gross_amount_eur,
        sum(discount_amount_eur)    as discount_amount_eur,
        sum(net_amount_eur)         as items_net_amount_eur,
        sum(cost_amount_eur)        as cost_amount_eur,
        sum(gross_margin_eur)       as gross_margin_eur
    from order_items
    group by order_id

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        customers.country_code,
        country_regions.region,
        orders.ordered_at,
        orders.order_date,
        orders.order_month,
        orders.order_status,
        orders.is_revenue_order,
        orders.channel,
        orders.currency,
        orders.campaign_code,
        orders.campaign_code is not null                                    as has_campaign,

        items.order_lines,
        items.units,

        -- order currency
        items.items_net_amount + orders.shipping_amount                     as order_total,

        -- EUR (reporting currency)
        items.items_gross_amount_eur,
        items.discount_amount_eur,
        items.items_net_amount_eur,
        orders.shipping_amount_eur,
        items.items_net_amount_eur + orders.shipping_amount_eur             as order_total_eur,
        items.cost_amount_eur,
        items.gross_margin_eur,
        case when orders.is_revenue_order then items.items_net_amount_eur else 0 end
                                                                            as net_revenue_eur,

        -- payments
        coalesce(payments.paid_amount, 0)                                   as paid_amount,
        coalesce(payments.refunded_amount, 0)                               as refunded_amount,
        coalesce(payments.failed_attempts, 0)                               as failed_payment_attempts,
        payments.payment_method,
        payments.paid_at,
        coalesce(payments.paid_amount, 0)
            >= items.items_net_amount + orders.shipping_amount              as is_fully_paid

    from orders
    left join items_per_order as items
        on orders.order_id = items.order_id
    left join payments
        on orders.order_id = payments.order_id
    left join customers
        on orders.customer_id = customers.customer_id
    left join country_regions
        on customers.country_code = country_regions.country_code

)

select * from final
