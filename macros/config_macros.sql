{#------------------------------------------------------------------------------------------------------#}
{#-- Global config macros for datavault4coalesce                                                     --#}
{#------------------------------------------------------------------------------------------------------#}
{%- macro datavault4coalesce__beginning_of_all_times(database_type) -%}
  {%- if database_type | upper == "SNOWFLAKE" -%}
    0001-01-01T00:00:01
  {%- elif database_type | upper == "DATABRICKS" -%}
    0001-01-01 00:00:01
  {%- endif %}
{%- endmacro -%}

{%- macro datavault4coalesce__end_of_all_times(database_type) -%}
  {%- if database_type | upper == "SNOWFLAKE" -%}
    8888-12-31T23:59:59
  {%- elif database_type | upper == "DATABRICKS" -%}
    8888-12-31 23:59:59
  {%- endif %}
{%- endmacro -%}

{%- macro datavault4coalesce__hash_passthrough_input_transformations(database_type) -%}
  {%- if database_type | upper == "SNOWFLAKE" -%}
    FALSE
  {%- elif database_type | upper == "DATABRICKS" -%}
    TRUE
  {%- endif %}
{%- endmacro -%}

{%- macro datavault4coalesce__timestamp_format(database_type) -%}
  {%- if database_type | upper == "SNOWFLAKE" -%}
    YYYY-MM-DDTHH24:MI:SS
  {%- elif database_type | upper == "DATABRICKS" -%}
    yyyy-MM-dd HH:mm:ss
  {%- endif %}
{%- endmacro -%}
