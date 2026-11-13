{{ config(materialized='table') }}

-- One row per campaign: orders, revenue and margin against the campaign budget.

with

fct_orders as (
    select * from {{ ref('fct_orders') }}
),

campaigns as (
    select * from {{ ref('campaigns') }}
),

campaign_orders as (

    select
        campaign_code,
        count(*)                                                            as orders,
        count_if(is_revenue_order)                                          as revenue_orders,
        count(distinct customer_id)                                         as customers,
        sum(net_revenue_eur)                                                as net_revenue_eur,
        sum(case when is_revenue_order then discount_amount_eur else 0 end) as discount_eur,
        sum(case when is_revenue_order then gross_margin_eur else 0 end)    as gross_margin_eur
    from fct_orders
    where campaign_code is not null
    group by campaign_code

),

final as (

    select
        campaigns.campaign_code,
        campaigns.campaign_name,
        campaigns.campaign_type,
        campaigns.start_date,
        campaigns.end_date,
        campaigns.budget_eur,
        coalesce(campaign_orders.orders, 0)                 as orders,
        coalesce(campaign_orders.revenue_orders, 0)         as revenue_orders,
        coalesce(campaign_orders.customers, 0)              as customers,
        coalesce(campaign_orders.net_revenue_eur, 0)        as net_revenue_eur,
        coalesce(campaign_orders.discount_eur, 0)           as discount_eur,
        coalesce(campaign_orders.gross_margin_eur, 0)       as gross_margin_eur,
        round(coalesce(campaign_orders.net_revenue_eur, 0) / nullif(campaigns.budget_eur, 0), 2)
                                                            as revenue_per_budget_eur,
        coalesce(campaign_orders.gross_margin_eur, 0) - campaigns.budget_eur
                                                            as margin_after_budget_eur,
        case
            when current_date() < campaigns.start_date then 'planned'
            when current_date() <= campaigns.end_date then 'running'
            else 'finished'
        end                                                 as campaign_state
    from campaigns
    left join campaign_orders
        on campaigns.campaign_code = campaign_orders.campaign_code

)

select * from final