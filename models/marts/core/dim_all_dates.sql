{{ config(
    materialized = 'view'
)}}

select
    date_day,
    date_trunc('month', date_day) as first_day_of_month,
    last_day(date_day) as last_day_of_month

from {{ ref('dim_date_spine') }}