with payments as (

    select * from {{ ref('stg_payments') }}

),

aggregated as (

    select
        payment_status,
        sum(payment_amount) as total_revenue

    from payments
    group by payment_status

)

select * from aggregated
order by payment_status asc