{{ config(enabled = true) }}

select
    customer_id,
    count(*) as customer_id_count

from {{ ref('dim_customers') }}
group by customer_id
    having customer_id_count > 1