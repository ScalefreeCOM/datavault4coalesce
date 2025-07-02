
{%- macro datavault4coalesce__snowflake_string_to_date(format, timestamp) -%}
  TO_DATE('{{ timestamp }}', '{{ format }}')
{%- endmacro -%}
