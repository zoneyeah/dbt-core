{% macro test_pkg_and_dispatch(model) -%}
  {{ return(adapter.dispatch('test_pkg_and_dispatch', macro_namespace = 'test_utils')()) }}
{%- endmacro %}

{% macro default__test_pkg_and_dispatch(model) %}
    select count(*) from {{ test_utils.current_timestamp() }}
{% endmacro %}
