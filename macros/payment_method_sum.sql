{%- macro payment_method_sum(payment_method) %}

    sum (
        case when  payment_method = '{{ payment_method }}' then payment_amount else 0 end
    ) as {{ payment_method | replace(' ', '_') }}_amt

{%- endmacro %}