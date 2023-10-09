{{
    config(
        materialized=  'table'
    )
}}


with

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}
    where payment_status != 'fail'

),

order_totals as (

    select
        order_id,
        payment_status,
        sum(payment_amount) as order_value_dollars
    
    from payments
    group by order_id, payment_status

),

final as (

    select
        o.*,
        t.payment_status,
        t.order_value_dollars

    from orders as o
        left join order_totals as t 
            on o.order_id = t.order_id

)

select * from final