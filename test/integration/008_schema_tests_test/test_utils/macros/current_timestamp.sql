{% macro _get_utils_namespaces() %}
  {% set override_namespaces = var('test_utils_dispatch_list', []) %}
  {% do return(override_namespaces + ['test_utils']) %}
{% endmacro %}

{% macro current_timestamp() -%}
  {{ return(adapter.dispatch('current_timestamp', packages = test_utils._get_utils_namespaces())()) }}
{%- endmacro %}

{% macro default__current_timestamp() -%}
  now()
{%- endmacro %}
