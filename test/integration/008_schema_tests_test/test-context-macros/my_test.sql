{% macro test_my_test(model) %}
    select count(*) from {{ local_utils.current_timestamp() }}
{% endmacro %}
