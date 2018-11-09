{{
  config(
    materialized = "udf",
    arguments = {"json_field": "varchar(2000)"},
    return_type = "integer",
    language = "sql"
  )
}}

select 1
