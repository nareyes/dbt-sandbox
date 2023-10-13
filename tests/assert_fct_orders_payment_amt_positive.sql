{{ config(enabled = true) }}

select
    payment_amount

from {{ ref('fct_orders') }}
where payment_amount < 0