with 

customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

customer_orders as (
    select
        customer_id,
        min (order_date) as first_order_date,
        max (order_date) as most_recent_order_date,
        count (order_id) as number_of_orders,
        sum (payment_amount) as lifetime_value
    from orders
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.first_order_date,
        o.most_recent_order_date,
        coalesce (o.number_of_orders, 0) as number_of_orders,
        coalesce (o.lifetime_value, 0) as lifetime_value
    from customers as c
        left join customer_orders as o using (customer_id)
)

select * from final