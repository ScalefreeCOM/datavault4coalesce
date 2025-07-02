
{#-- Utility macro: check if object exists, is defined and not empty --#}
{%- macro datavault4coalesce__databricks_is_something(obj) -%}
    {%- if obj is not none and obj is defined and obj -%}
        {{ true }}
    {%- else -%}
        {{ false }}
    {%- endif -%}
{%- endmacro -%}