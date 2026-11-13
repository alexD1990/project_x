{{ config(materialized='table') }}

with products as (

    select * from {{ ref('stg_shop__products') }}

),

product_categories as (

    select * from {{ ref('product_categories') }}

),

order_items as (

    select * from {{ ref('int_order_items__enriched') }}

),

sales as (

    select
        product_sku,
        sum(quantity)           as units_sold,
        count(distinct order_id) as orders,
        sum(net_amount_eur)     as net_revenue_eur,
        sum(gross_margin_eur)   as gross_margin_eur,
        max(order_date)         as last_sold_date
    from order_items
    where is_revenue_order
    group by product_sku

),

final as (

    select
        products.product_sku,
        products.product_name,
        products.brand,
        products.category_code,
        product_categories.category_name,
        product_categories.department,
        products.list_price_eur,
        products.cost_price_eur,
        products.is_active,
        products.launched_at,

        coalesce(sales.units_sold, 0)           as units_sold,
        coalesce(sales.orders, 0)               as orders,
        coalesce(sales.net_revenue_eur, 0)      as net_revenue_eur,
        coalesce(sales.gross_margin_eur, 0)     as gross_margin_eur,
        round(sales.gross_margin_eur / nullif(sales.net_revenue_eur, 0), 4)
                                                as gross_margin_pct,
        sales.last_sold_date

    from products
    left join product_categories
        on products.category_code = product_categories.category_code
    left join sales
        on products.product_sku = sales.product_sku

)

select * from final
