{% set old_etl_relation = ref('leg_customer_orders') -%}

{% set dbt_relation = ref('fct_customer_orders') %}

{{
    audit_helper.compare_relations(
        a_relation = old_etl_relation,
        b_relation = dbt_relation,
        primary_key = "order_id",
        summarize = false,
        excluse_columns = [
            'full_name',
            'non_returned_order_count',
            'avg_non_returned_order_value'
        ]
    )
}}