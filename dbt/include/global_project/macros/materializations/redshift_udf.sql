{% materialization udf, default %}

    {%- set identifier = model['alias'] -%}
    {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      type='view') -%}

    {% call statement('find_udf_types', fetch_result=True) %}
        with schema as (
            select
                pg_namespace.oid as id,
                pg_namespace.nspname as name
            from pg_namespace
            where nspname != 'information_schema' and nspname not like 'pg_%'
        )
        select
            oid as id,
            typname
        from pg_type
    {% endcall %}

    {% call statement('find_udfs', fetch_result=True) %}
        with schema as (
            select
                pg_namespace.oid as id,
                pg_namespace.nspname as name
            from pg_namespace
            where nspname != 'information_schema' and nspname not like 'pg_%'
        )
        select
            proname,
            schema.name,
            proargtypes
        from pg_proc
        left join schema on pg_proc.pronamespace = schema.id
            where proname ilike '%{{identifier}}%'
            and schema.name ilike '{{schema}}'
    {% endcall %}

    {% set arg_type_lookup = {} %}
    {% set udf_types = load_result('find_udf_types')['data'] %}

    {% for (type_id, type_name) in udf_types %}
        {% set _ = arg_type_lookup.update({type_id: type_name}) %}
    {% endfor %}

    {{log(arg_type_lookup, true)}}

    {% set matching_udfs = load_result('find_udfs')['data'] %}

    {{log(matching_udfs, true)}}

    {% for matching_udf in matching_udfs %}

        drop function {{ matching_udf[1] }}.{{ matching_udf[0] }}
        (
        {% for arg_type_id in (matching_udf[2]|string).split() %}
            {{ arg_type_lookup[arg_type_id] }}
            {% if not loop.last %},{% endif %}
        {% endfor %}
        )
        cascade;
    {% endfor %}

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
