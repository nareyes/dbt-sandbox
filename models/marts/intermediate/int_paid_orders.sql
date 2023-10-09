{{
    config(
        materialized = 'table'
    )
}}


with 

orders as (

  select * from {{ ref('stg_orders') }}

),

payments as (

  select * from {{ ref('stg_payments') }}

),

completed_payments as (

    select 
        order_id,
        max(created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1

),

paid_orders as (

    select 
        o.order_id,
        o.customer_id,
        o.order_date as order_placed_at,
        o.order_status,
        p.total_amount_paid,
        p.payment_finalized_date
    from orders as o
        left join completed_payments as p
            on o.order_id = p.order_id
)

select * from paid_orders