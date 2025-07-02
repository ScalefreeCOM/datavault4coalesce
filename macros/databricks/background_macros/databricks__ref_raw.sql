
{%- macro datavault4coalesce__databricks_ref_raw(location_name, node_name) -%}
    {% raw %}{{ ref('{% endraw %}{{ location_name }}{% raw %}', '{% endraw %}{{ node_name }}{% raw %}') }}{% endraw %}
{%- endmacro -%}