{% materialization udf, default %}

    {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      type='view') -%}

    -- put matching udf here
    {%- set identifier = model['alias'] -%}

    {% call statement('find_udfs', fetch_result=True) %}
        select * from pg_proc where proname ilike '%{{identifier}}%'
    {% endcall %}

    {% set matching_udfs = load_result('find_udfs')['data'] %}

    {% if matching_udfs.length > 0 %}
        drop function {{ matching_udf }} cascade;
    {% endif %}

    {% call statement('main') %}

        create function {{ target_relation }}
            (
                {% for name, type in config.require('arguments').items() %}
                    {{name}} {{type}}
                {% endfor %}
            )
        returns {{ config.require('return_type') }}
        stable as $$
            {{ sql }}
        $$ language {{ config.require('language') }}

    {% endcall %}

    {{ adapter.commit() }}

{% endmaterialization %}
