{{
    config(
        materialized = 'table'
    )
}}


{%-
    set payment_methods = [
        'bank_transfer',
        'coupon',
        'credit_card',
        'gift_card'
    ]
-%}


with

payments as (
    select * from {{ ref('stg_payments') }}
),

{%- set method_sums = [] -%}

{%- for payment_method in payment_methods -%}

    {%- do method_sums.append(payment_method_sum(payment_method)) -%}

{%- endfor %}

pivoted as (
    select
        order_id,
        {{ method_sums | join(',\n      ') }}
        
    from payments
    where status = 'success'
    group by order_id
)

select * 
from pivoted