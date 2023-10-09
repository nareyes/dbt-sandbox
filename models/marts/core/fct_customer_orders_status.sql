{{
    config(
        materialized = 'view'
    )
}}

with 

customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as ( 

    select * from {{ ref('stg_payments') }}

),

paid_orders as (

    select * from {{ ref('int_paid_orders') }}

),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_placed_at,
        o.order_status,
        o.total_amount_paid,
        o.payment_finalized_date,
        c.last_name as customer_last_name,
        c.first_name as customer_first_name,

        -- sales transaction sequence
        row_number() over (order by o.order_id) as transaction_seq,

        -- customers sales sequence
        row_number() over (partition by o.customer_id order by o.order_id) as customer_sales_seq,

        -- new vs returning customers
        case
            when (rank() over (partition by o.customer_id order by o.order_placed_at, o.order_id) = 1) then 'new'
            else 'return'
        end as nvsr,

        -- customer lifetime value
        sum (total_amount_paid) over (partition by o.customer_id order by o.order_placed_at) as customer_lifetime_value,

        -- first day of sale
        first_value (order_placed_at) over (partition by o.customer_id order by o.order_placed_at) as fdos

    from paid_orders as o
        left join customers as c
            on o.customer_id = c.customer_id
)

select * from final