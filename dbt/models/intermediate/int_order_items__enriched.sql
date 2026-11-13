-- Order lines with product, category and EUR amounts. Lines whose SKU is missing
-- from the catalogue are kept and labelled 'Unknown'.

with order_items as (

    select * from {{ ref('stg_shop__order_items') }}

),

orders as (

    select * from {{ ref('int_orders__converted') }}

),

products as (

    select * from {{ ref('stg_shop__products') }}

),

product_categories as (

    select * from {{ ref('product_categories') }}

),

enriched as (

    select
        order_items.order_item_id,
        order_items.order_id,
        orders.customer_id,
        orders.order_date,
        orders.order_month,
        orders.order_status,
        orders.is_revenue_order,
        orders.currency,
        order_items.product_sku,
        coalesce(products.product_name, 'Unknown product')         as product_name,
        coalesce(products.brand, 'Unknown')                         as brand,
        coalesce(product_categories.category_name, 'Unknown')       as category_name,
        coalesce(product_categories.department, 'Unknown')          as department,
        order_items.quantity,
        order_items.discount_pct,
        order_items.net_amount,
        cast(order_items.gross_amount    * orders.rate_to_eur as decimal(12, 2))   as gross_amount_eur,
        cast(order_items.discount_amount * orders.rate_to_eur as decimal(12, 2))   as discount_amount_eur,
        cast(order_items.net_amount      * orders.rate_to_eur as decimal(12, 2))   as net_amount_eur,
        cast(order_items.quantity * products.cost_price_eur as decimal(12, 2))     as cost_amount_eur
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    left join products
        on order_items.product_sku = products.product_sku
    left join product_categories
        on products.category_code = product_categories.category_code

)

select
    *,
    net_amount_eur - coalesce(cost_amount_eur, 0) as gross_margin_eur
from enriched
