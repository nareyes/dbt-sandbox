{{
    config(
        materialized = 'view'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'order_date']) }} as id,
    customer_id,
    order_date,
    count (*) as daily_order_count

from {{ ref('stg_orders') }}
group by 1, customer_id, order_date