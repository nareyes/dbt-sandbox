{{
    config(
        materialized = 'view'
    )
}}


with

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

order_payments as (

    select
        order_id,
        sum (case when payment_status = 'success' then payment_amount end) as payment_amount

    from payments
    group by order_id
),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        coalesce (p.payment_amount, 0) as payment_amount
        
    from orders as o
        left join order_payments as p using (order_id)
)

select * from final