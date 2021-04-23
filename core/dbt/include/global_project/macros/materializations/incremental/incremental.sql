
{% materialization incremental, default -%}

  {% set unique_key = config.get('unique_key') %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change')) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or should_full_refresh() %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}

      {% set schema_changed = check_for_schema_changes(tmp_relation, target_relation) %}

      {% if schema_changed %}
      
        {% if on_schema_change=='fail' %}
          {{ 
            exceptions.raise_compiler_error('The source and target schemas on this incremental model are out of sync!
               Please re-run the incremental model with full_refresh set to True to update the target schema.
               Alternatively, you can update the schema manually and re-run the process.') 
          }}
      
        {# unless we ignore, run the sync operation per the config #}
        {% elif on_schema_change != 'ignore' %}
        
          {% set schema_changes = sync_columns(tmp_relation, target_relation, on_schema_change) %}

        {% endif %}

      {% endif %}

      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}

      {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
  
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
