{{ config(materialized='table') }}

with customers as (

    select * from {{ ref('stg_shop__customers') }}

),

country_regions as (

    select * from {{ ref('country_regions') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

customer_orders as (

    select
        customer_id,
        min(order_date)                                         as first_order_date,
        max(order_date)                                         as last_order_date,
        count_if(is_revenue_order)                              as lifetime_orders,
        count_if(order_status = 'returned')                     as returned_orders,
        sum(net_revenue_eur)                                    as lifetime_revenue_eur,
        sum(case when is_revenue_order then gross_margin_eur else 0 end)
                                                                as lifetime_margin_eur
    from orders
    group by customer_id

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        customers.country_code,
        country_regions.country_name,
        country_regions.region,
        customers.city,
        customers.signup_date,
        customers.is_marketing_opt_in,

        customer_orders.first_order_date,
        customer_orders.last_order_date,
        coalesce(customer_orders.lifetime_orders, 0)            as lifetime_orders,
        coalesce(customer_orders.returned_orders, 0)            as returned_orders,
        coalesce(customer_orders.lifetime_revenue_eur, 0)       as lifetime_revenue_eur,
        coalesce(customer_orders.lifetime_margin_eur, 0)        as lifetime_margin_eur,
        case
            when coalesce(customer_orders.lifetime_orders, 0) > 0
            then round(customer_orders.lifetime_revenue_eur / customer_orders.lifetime_orders, 2)
        end                                                     as avg_order_value_eur,

        case
            when coalesce(customer_orders.lifetime_orders, 0) = 0 then 'prospect'
            when customer_orders.lifetime_orders = 1 then 'one_time'
            when customer_orders.lifetime_orders < 5 then 'returning'
            else 'loyal'
        end                                                     as customer_segment,
        datediff(current_date(), customer_orders.last_order_date) <= 90
                                                                as is_active_last_90_days

    from customers
    left join country_regions
        on customers.country_code = country_regions.country_code
    left join customer_orders
        on customers.customer_id = customer_orders.customer_id

)

select * from final
