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

    select * from {{ ref('int_orders') }}

),

customer_orders as (

    select
        o.*,
        c.first_name,
        c.last_name,
        c.full_name,
        min(o.order_date) over(partition by o.customer_id) as first_order_date,
        min(o.valid_order_date) over(partition by o.customer_id) as first_non_returned_order_date,
        max(o.valid_order_date) over(partition by o.customer_id) as most_recent_non_returned_order_date,
        count(*) over(partition by o.customer_id) as order_count,
        sum(nvl2(o.valid_order_date, 1, 0)) over(partition by o.customer_id) as non_returned_order_count,
        sum(nvl2(o.valid_order_date, o.order_value_dollars, 0)) over(partition by o.customer_id) as total_lifetime_value,
        array_agg(distinct o.order_id) over(partition by o.customer_id) as order_ids

    from orders as o
        inner join customers as c
            on o.customer_id = c.customer_id
),

final as (

    select
        order_id,
        customer_id,
        first_name,
        last_name,
        full_name,
        first_order_date,
        order_count,
        total_lifetime_value,
        non_returned_order_count,
        total_lifetime_value / nullifzero(non_returned_order_count) as avg_non_returned_order_value,
        order_value_dollars,
        order_status,
        payment_status

    from customer_orders
        
)

select * from final