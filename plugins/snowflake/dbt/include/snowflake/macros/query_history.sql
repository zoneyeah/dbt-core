{% macro snowflake__get_query_history(information_schema, last_queried_at) -%}

  use warehouse analytics;
  {% set query %}
    select 

      query_id,
      query_text,
      database_name,
      schema_name,
      session_id,
      user_name,
      execution_status,
      start_time,
      end_time

    from table(
        information_schema.query_history(
            dateadd('hours',-1,current_timestamp()), --start_timestamp, use last_queried_at here maybe
            current_timestamp(),                     --end_timestamp
            1000)                                    --row count
            )

    order by start_time

  {%- endset -%}

  {{ return(run_query(query)) }}

{%- endmacro %}
