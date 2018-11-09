{% materialization udf, default %}

    {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      type='view') -%}

    -- put matching udf here
    {%- set identifier = model['alias'] -%}

    {% call statement('find_udfs', fetch_result=True) %}
        with schema as (
            select
                pg_namespace.oid as id,
                pg_namespace.nspname as name
            from pg_namespace
            where nspname != 'information_schema' and nspname not like 'pg_%'
        ),
        select *
        from pg_proc
        left join schema on pg_proc.pronamespace = schema.id
            where proname ilike '%{{identifier}}%'
            and schema ilike '{{schema}}'
    {% endcall %}

    {% set matching_udfs = load_result('find_udfs')['data'] %}

    {% if matching_udfs.length > 0 %}
        drop function {{ target_relation }} cascade;
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
