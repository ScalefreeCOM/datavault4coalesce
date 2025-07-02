
{%- macro datavault4coalesce__databricks_string_to_date(format, timestamp) -%}
  TO_DATE('{{ timestamp }}', '{{ format }}')
{%- endmacro -%}
