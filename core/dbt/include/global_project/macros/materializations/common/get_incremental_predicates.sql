{#

    These macros will compile the proper join predicates for incremental models,
    merging default behavior with the optional, user-supplied incremental_predicates
    config. 

#}


{% macro get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates, partitions=none) %}
    {{ adapter.dispatch('get_incremental_predicates')(target_relation, incremental_strategy, unique_key, user_predicates, partitions) }}
{% endmacro %}

{% macro default__get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates=none, partitions=none) %}
    {# this should be the default added to the `delete from` syntax in the basic strategy#}
    {% if user_predicates %}
        {%- set predicates -%}
            {%- for condition in user_predicates %}
                and {{ target_relation.name }}.{{ condition.source_col }} {{ condition.expression }}
            {%- endfor %} 
        {%- endset -%}
    {% endif %}

    {{ return(predicates) }}

{% endmacro %}

{%- macro snowflake__get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates=none, partitions=none) %}

    {%- if incremental_strategy == 'merge'%}
        {%- if unique_key -%}
            {%- set match_criteria %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}  
            {%- endset -%}
        {% else %}
            {%- set match_criteria %}
                FALSE
            {%- endset -%}
        {% endif %}
    
        {%- if user_predicates -%}
            {%- set filter_criteria %}
                {%- for condition in user_predicates -%}
                    and DBT_INTERNAL_DEST.{{ condition.source_col }} {{ condition.expression }}
                {%- endfor -%}
            {%- endset -%}
        {% endif %}
    {%- elif incremental_strategy == 'delete+insert' %}
        {%- if user_predicates -%}
            {%- set filter_criteria %}
                {%- for condition in user_predicates -%}
                    and {{ target_relation.name }}.{{ condition.source_col }} {{ condition.expression }}
                {%- endfor -%}
            {%- endset -%}
        {% endif %}
    {%- endif -%}

    {{ match_criteria }}
    {{ filter_criteria }}

{%- endmacro -%}

{% macro bigquery__get_incremental_predicates(target_relation, incremental_strategy, unique_key, user_predicates=none, partitions=none) %}
    
    {%- if incremental_strategy == 'insert_overwrite'%}
        {% if partitions is not none and partitions != [] %} {# static #}
            {%- set match_criteria %}
                {{ partition_by.render(alias='DBT_INTERNAL_DEST') }} in (
                    {{ partitions | join (', ') }}
                )
            {%- endset -%}
        {% else %}
            {%- set match_criteria %} {# dynamic #}
                {{ partition_by.render(alias='DBT_INTERNAL_DEST') }} in unnest(dbt_partitions_for_replacement)
            {%- endset -%}
        {% endif %}
    
        {%- if user_predicates -%}
            {%- set filter_criteria %}
                {%- for condition in user_predicates -%}
                    and DBT_INTERNAL_DEST.{{ condition.source_col }} {{ condition.expression }}
                {%- endfor -%}
            {%- endset -%}
        {% endif %}

        {{ match_criteria | join(' and ') }}
        {{ filter_criteria }}


    {%- elif incremental_strategy == 'merge' %}
        {%- if unique_key -%}
            {%- set match_criteria %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}  
            {%- endset -%}
        {% else %}
            {%- set match_criteria %}
                FALSE
            {%- endset -%}
        {% endif %}
    
        {%- if user_predicates -%}
            {%- set filter_criteria %}
                {%- for condition in user_predicates -%}
                    and DBT_INTERNAL_DEST.{{ condition.source_col }} {{ condition.expression }}
                {%- endfor -%}
            {%- endset -%}
        {% endif %}

        {{ match_criteria }}
        {{ filter_criteria }}

    {%- endif -%}

{%- endmacro -%}