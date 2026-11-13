-- A completed order must have successful payments covering the order total.
-- Returns the orders that break the rule; the test passes when it returns no rows.

select
    order_id,
    order_total,
    paid_amount
from {{ ref('fct_orders') }}
where order_status = 'completed'
  and not is_fully_paid
