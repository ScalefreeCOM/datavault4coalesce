{%- macro datavault4coalesce__beginning_of_all_times() %}

{%- set beginning_of_all_times = parameters.datavault4coalesce__beginning_of_all_times -%}

{{ beginning_of_all_times }}

{%- endmacro -%}


{%- macro datavault4coalesce__end_of_all_times() %}

{%- set end_of_all_times = parameters.datavault4coalesce__end_of_all_times -%} 

{{ end_of_all_times }}

{%- endmacro -%}


{%- macro datavault4coalesce__string_to_timestamp(format, timestamp) -%}
    TO_TIMESTAMP('{{ timestamp }}', '{{ format }}')
{%- endmacro -%}


{%- macro datavault4coalesce__timestamp_format() %}

{%- set timestamp_format = parameters.datavault4coalesce__timestamp_format -%}

{{ timestamp_format }}

{%- endmacro -%}


{%- macro datavault4coalesce__ghost_record_per_datatype(column_name, datatype, ghost_record_type, col_size) -%}

{%- set beginning_of_all_times = datavault4coalesce__beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4coalesce__end_of_all_times() -%}
{%- set timestamp_format = datavault4coalesce__timestamp_format() -%}

{%- if ghost_record_type == 'unknown' -%}
     {%- if datatype in ['TIMESTAMP_NTZ','TIMESTAMP', 'DATE'] %}{{ datavault4coalesce__string_to_timestamp(timestamp_format, beginning_of_all_times) }} AS {{ column_name }}
     {% elif datatype.upper().startswith('STRING') or datatype.upper().startswith('VARCHAR') %}'(unknown)' AS {{ column_name }}
     {% elif datatype in ['NUMBER','INT','FLOAT','DECIMAL'] or datatype.upper().startswith('NUMBER') %}0 AS {{ column_name }}
     {% elif datatype == 'BOOLEAN' %}CAST('FALSE' AS BOOLEAN) AS {{ column_name }}
     {% else %}NULL AS {{ column_name }}
     {% endif %}
{%- elif ghost_record_type == 'error' -%}
     {%- if datatype in ['TIMESTAMP_NTZ','TIMESTAMP', 'DATE'] %}{{ datavault4coalesce__string_to_timestamp(timestamp_format, end_of_all_times) }} AS {{ column_name }}
     {% elif datatype.upper().startswith('STRING') or datatype.upper().startswith('VARCHAR') %}'(error)' AS {{ column_name }}
     {% elif datatype in ['NUMBER','INT','FLOAT','DECIMAL'] or datatype.upper().startswith('NUMBER') %}-1 AS {{ column_name }}
     {% elif datatype == 'BOOLEAN' %}CAST('FALSE' AS BOOLEAN) AS {{ column_name }}
     {% else %}NULL AS {{ column_name }}
      {% endif %}
{%- endif -%}

{%- endmacro -%}