{% macro clean_stale_models(database = target.database, schema = target.schema, days = 7, dry_run = True) %}
    
    {% set get_drop_commands_query %}
        select
            case 
                when table_type = upper('view') then table_type
                else upper('table')
            end as drop_type, 
            upper('drop ') || drop_type || ' {{ database | upper }}.' || table_schema || '.' || table_name || ';'
        from {{ database }}.information_schema.tables 
        where table_schema = upper('{{ schema }}')
        and last_altered <= current_date - {{ days }} 
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info = True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {% for query in drop_queries %}

        {% if dry_run %}
            {{ log(query, info = True) }}

        {% else %}
            {{ log('dropping object with command: ' ~ query, info = True) }}
            {% do run_query(query) %} 

        {% endif %}  

    {% endfor %}
    
{% endmacro %} 