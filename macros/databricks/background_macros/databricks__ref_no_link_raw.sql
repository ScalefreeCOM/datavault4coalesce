
{%- macro datavault4coalesce__databricks_ref_no_link_raw(location_name, node_name) -%}
    {% raw %}{{ ref_no_link('{% endraw %}{{ location_name }}{% raw %}', '{% endraw %}{{ node_name }}{% raw %}') }}{% endraw %}
{%- endmacro -%}