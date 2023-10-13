{{ config(enabled = true) }}

select
    customer_id,
    avg(payment_amount) as average_amount

from {{ ref('fct_orders') }}
group by customer_id
    having count(customer_id) > 1 and average_amount < 1