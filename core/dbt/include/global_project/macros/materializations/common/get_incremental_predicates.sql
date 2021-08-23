{% macro get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates) %}
    {{ adapter.dispatch('get_incremental_predicates')(target_relation, incremental_strategy, unique_key, user_predicates) }}
{% endmacro %}

{% macro default__get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates) %}

    {% if user_predicates %}
        {%- set predicates -%}
            {%- for condition in user_predicates %}
                and {{ target_relation.name }}.{{ condition.source_col }} {{ condition.expression }}
            {%- endfor %} 
        {%- endset -%}
    {% endif %}

    {{ return(predicates) }}

{% endmacro %}

{% macro snowflake__get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates=none) %}
    {{ return('snowflake') }}
{% endmacro %}

{% macro bigquery__get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates=none) %}
    {{ return('bigquery') }}
{% endmacro %}