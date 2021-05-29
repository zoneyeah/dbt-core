{% macro test_call_pkg_macro(model) %}
    select count(*) from {{ test_utils.current_timestamp() }}
{% endmacro %}
