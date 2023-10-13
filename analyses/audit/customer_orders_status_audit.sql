{# compare relations #}
{% set old_etl_relation = ref('leg_customer_orders_status') -%}

{% set dbt_relation = ref('fct_customer_orders_status') %}

{{
    audit_helper.compare_relations(
        a_relation = old_etl_relation,
        b_relation = dbt_relation,
        primary_key = "order_id",
        summarize = true
    )
}}