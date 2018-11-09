{% materialization udf, redshift %}

    -- put matching udf here
    {%- set identifier = model['alias'] -%}
    {% set matching_udf = null %}

    {% if matching_udf %}
        drop function {{ matching_udf }} cascade;
    {% endif %}

    create function {{ model.alias }}
        (
            {% for name, type in config.require('arguments').items() %}
                {{name}} {{type}}
            {% endfor %}
        )
    returns {{ config.require('return_type') }}
    stable as $$
        {{ sql }}
    $$ language {{ config.require('language') }}


{% endmaterialization %}
