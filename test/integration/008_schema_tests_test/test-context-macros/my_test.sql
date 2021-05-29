{% macro test_call_pkg_macro(model) %}
    select count(*) from {{ local_utils.current_timestamp() }}
{% endmacro %}
