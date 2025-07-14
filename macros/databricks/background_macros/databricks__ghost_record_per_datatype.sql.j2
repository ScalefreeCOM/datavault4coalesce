
{%- macro datavault4coalesce__databricks_ghost_record_per_datatype(column_name, datatype, ghost_record_type, hash, hash_algo=none) -%}

{%- set beginning_of_all_times = datavault4coalesce.config.beginning_of_all_times | replace('"', '') -%}
{%- set end_of_all_times = datavault4coalesce.config.end_of_all_times | replace('"', '') -%}
{%- set timestamp_format = datavault4coalesce.config.timestamp_format | replace('"', '') -%}
{%- set date_format = datavault4coalesce.config.date_format -%}
{%- set beginning_of_all_times_date = datavault4coalesce.config.beginning_of_all_times_date -%}
{%- set end_of_all_times_date = datavault4coalesce.config.end_of_all_times_date -%}
{%- set unknown_value__STRING = datavault4coalesce.config.unknown_value__STRING -%}
{%- set error_value__STRING = datavault4coalesce.config.error_value__STRING -%}

{%- if hash %}
{%- set datatype = datatype|upper -%}
{%- set hash_alg = datavault4coalesce__databricks_hash_algorithm(hash_function=hash_algo, hash_datatype=datatype) -%}
{%- set unknown_key = datavault4coalesce__databricks_unknown_key(hash_function=hash_algo, hash_datatype=datatype) -%}
{%- set error_key = datavault4coalesce__databricks_error_key(hash_function=hash_algo, hash_datatype=datatype) -%}

{%- if ghost_record_type == 'unknown' -%}
      {{ unknown_key }} AS {{ column_name }}
{%- elif ghost_record_type == 'error' -%}
      {{ error_key }} AS {{ column_name }}
{%- else -%}
      {%- if execute -%}
      {{ exceptions.raise_compiler_error("Invalid Ghost Record Type. Accepted are 'unknown' and 'error'.") }}
      {%- endif %}
{%- endif %}

{%- else %}
{%- set datatype = datatype | string | upper | trim -%}
{%- if ghost_record_type == 'unknown' -%}
      {%- if datatype == 'TIMESTAMP' %}
      {{ datavault4coalesce__databricks_string_to_timestamp(timestamp_format, beginning_of_all_times) }} AS {{ column_name }}
      {%- elif datatype == 'DATE' %}
      {{ datavault4coalesce__databricks_string_to_date(date_format, beginning_of_all_times) }} AS {{ column_name }}
      {%- elif datatype == 'STRING' %}
      {{ unknown_value__STRING }} AS {{ column_name }}
      {%- elif datatype in ['INT', 'SMALLINT', 'TINYINT', 'BIGINT', 'DOUBLE', 'FLOAT'] %}
      CAST(0 AS {{ datatype }}) AS {{ column_name }}
      {%- elif datatype.startswith('DECIMAL') %}
      CAST(0 AS DECIMAL) AS {{ column_name }}
      {%- elif datatype == 'BOOLEAN' %}
      CAST('FALSE' AS BOOLEAN) AS {{ column_name }}
      {%- elif datatype == 'BINARY' %}
      {{ unknown_key }} AS {{ column_name }}
      {%- else %}
      CAST(NULL AS {{ datatype }}) AS {{ column_name }}
      {%- endif %}

{%- elif ghost_record_type == 'error' -%}
      {%- if datatype == 'TIMESTAMP' %}
      {{ datavault4coalesce__databricks_string_to_timestamp(timestamp_format, end_of_all_times) }} AS {{ column_name }}
      {%- elif datatype == 'DATE' %}
      {{ datavault4coalesce__databricks_string_to_date(date_format, end_of_all_times) }} AS {{ column_name }}
      {%- elif datatype == 'STRING' %}
      {{ error_value__STRING }} AS {{ column_name }}
      {%- elif datatype in ['INT', 'SMALLINT', 'TINYINT', 'BIGINT', 'DOUBLE', 'FLOAT'] %}
      CAST(-1 AS {{ datatype }}) AS {{ column_name }}
      {%- elif datatype.startswith('DECIMAL') %}
      CAST(-1 AS DECIMAL) AS {{ column_name }}
      {%- elif datatype == 'BOOLEAN' %}
      CAST('FALSE' AS BOOLEAN) AS {{ column_name }}
      {%- elif datatype == 'BINARY' %}
      {{ error_key }} AS {{ column_name }}
      {%- else %}
      CAST(NULL AS {{ datatype }}) AS {{ column_name }}
      {%- endif %}

{%- else -%}
      {%- if execute -%}
      {{ exceptions.raise_compiler_error("Invalid Ghost Record Type. Accepted are 'unknown' and 'error'.") }}
      {%- endif %}
{%- endif %}
{%- endif %}

{%- endmacro %}
