{{ config(materialized='table') }}

-- Revenue and margin per month, sales region and department. Only completed and shipped orders count.

with order_items as (

    select * from {{ ref('int_order_items__enriched') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

),

final as (

    select
        order_items.order_month,
        coalesce(customers.region, 'Unknown')       as region,
        order_items.department,
        count(distinct order_items.order_id)        as orders,
        count(distinct order_items.customer_id)     as customers,
        sum(order_items.quantity)                   as units,
        sum(order_items.net_amount_eur)             as net_revenue_eur,
        sum(order_items.discount_amount_eur)        as discount_eur,
        sum(order_items.gross_margin_eur)           as gross_margin_eur,
        round(sum(order_items.gross_margin_eur) / nullif(sum(order_items.net_amount_eur), 0), 4)
                                                    as gross_margin_pct
    from order_items
    left join customers
        on order_items.customer_id = customers.customer_id
    where order_items.is_revenue_order
    group by all

)

select * from final
