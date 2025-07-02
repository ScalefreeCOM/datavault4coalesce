
{#-- Utility macro: wrap string input between escape characters --#}
{%- macro datavault4coalesce__databricks_escape_column_name(column) -%}
    {%- set escape_char_left  = '`' -%}
    {%- set escape_char_right = '`' -%}
    {%- set escaped_column_name = escape_char_left ~ column|upper|replace(escape_char_left, '')|replace(escape_char_right, '')|trim ~ escape_char_right|indent(4) -%}
    
  {{ escaped_column_name }}
{%- endmacro -%}