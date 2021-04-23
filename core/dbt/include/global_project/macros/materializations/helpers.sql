{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction)  %}
    {% if not inside_transaction and loop.first %}
      {% call statement(auto_begin=inside_transaction) %}
        commit;
      {% endcall %}
    {% endif %}
    {% set rendered = render(hook.get('sql')) | trim %}
    {% if (rendered | length) > 0 %}
      {% call statement(auto_begin=inside_transaction) %}
        {{ rendered }}
      {% endcall %}
    {% endif %}
  {% endfor %}
{% endmacro %}


{% macro column_list(columns) %}
  {%- for col in columns %}
    {{ col.name }} {% if not loop.last %},{% endif %}
  {% endfor -%}
{% endmacro %}


{% macro column_list_for_create_table(columns) %}
  {%- for col in columns %}
    {{ col.name }} {{ col.data_type }} {%- if not loop.last %},{% endif %}
  {% endfor -%}
{% endmacro %}


{% macro make_hook_config(sql, inside_transaction) %}
    {{ tojson({"sql": sql, "transaction": inside_transaction}) }}
{% endmacro %}


{% macro before_begin(sql) %}
    {{ make_hook_config(sql, inside_transaction=False) }}
{% endmacro %}


{% macro in_transaction(sql) %}
    {{ make_hook_config(sql, inside_transaction=True) }}
{% endmacro %}


{% macro after_commit(sql) %}
    {{ make_hook_config(sql, inside_transaction=False) }}
{% endmacro %}


{% macro drop_relation_if_exists(relation) %}
  {% if relation is not none %}
    {{ adapter.drop_relation(relation) }}
  {% endif %}
{% endmacro %}


{% macro load_relation(relation) %}
  {% do return(adapter.get_relation(
    database=relation.database,
    schema=relation.schema,
    identifier=relation.identifier
  )) -%}
{% endmacro %}


{% macro should_full_refresh() %}
  {% set config_full_refresh = config.get('full_refresh') %}
  {% if config_full_refresh is none %}
    {% set config_full_refresh = flags.FULL_REFRESH %}
  {% endif %}
  {% do return(config_full_refresh) %}
{% endmacro %}


{% macro incremental_validate_on_schema_change(on_schema_change, default_value='ignore') %}
   
   {% if on_schema_change not in ['sync', 'append', 'fail', 'ignore'] %}
     {{ return(default_value) }}

   {% else %}
     {{ return(on_schema_change) }}
   
   {% endif %}

{% endmacro %}

{% macro diff_columns(array_one, array_two) %}
  {% set result = [] %}
   {%- for col in array_one -%} 
     {%- if col not in array_two -%}
      {{ result.append(col) }}
    {%- endif -%}
   {%- endfor -%}
   {{ return(result) }}
{% endmacro %}

{% macro sync_columns(on_schema_change, source_relation, target_relation) %}

  {# if on_schema_change is append or sync, we perform the according action. Otherwise this is a noop #}
  
  {%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}
  {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set add_to_target_arr = diff_columns(source_columns, target_relation) %}
  {%- set remove_from_target_arr = diff_columns(target_relation, source_columns) %}
  
  {%- if on_schema_change in ['append', 'sync'] %}
    {%- for col in add_to_target_arr -%}
       {%- set build_sql = 'ALTER TABLE ' + target_relation.schema+'.'+target_relation.name + ' ADD COLUMN ' + col.name + ' ' + col.dtype -%}
       {%- do run_query(build_sql) -%}
    {%- endfor -%}
  {% endif %}
  
  {% if on_schema_change == 'sync' %}
    {%- for col in remove_from_target_arr -%}
      {%- set build_sql = 'ALTER TABLE ' + target_relation.schema+'.'+target_relation.name + ' DROP COLUMN ' + col.name -%}
      {%- do run_query(build_sql) -%}
    {%- endfor -%}
  {% endif %}
  
  -- check whether the schema changed
  {% if add_to_target_arr != [] or remove_from_target_arr != [] %}
    {%- set schema_changed = True -%}
  {% else %}
    {%- set schema_changed = False -%}
  {% endif %}

  -- return the list of columns added so we can set defaults if we have them
  {{ 
      return(
             {
              'schema_changed': schema_changed,
              'new_columns': add_to_target_arr
             }
          )
  }}
  
{% endmacro %}