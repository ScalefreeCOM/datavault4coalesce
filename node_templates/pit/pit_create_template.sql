{{ stage('Create PIT Table') }}

{%- set ns = namespace(sdts_datatype = '') %}

CREATE OR REPLACE TABLE {{ ref_no_link(node.location.name, node.name) }}
(
    {% for col in sources[0].columns %}
        {% if col.sourceColumns[0].column.is_Hub_hk or col.sourceColumns[0].column.is_Hub_hk or col.sourceColumns[0].column.is_Hub_ldts or col.sourceColumns[0].column.is_Link_ldts %}
            {{ col.name }} {{ col.dataType }}
        {% elif col.name == datavault4coalesce.config.sdts_alias %}
            {{ datavault4coalesce.config.sdts_alias }} {{ col.dataType }}
        {% else %}
            {{ col.sourceColumns[0].node.name }}_{{ col.name }} {{ col.dataType }}
        {% endif %}
        {% if not loop.last %} , {% endif %}
    {% endfor %}
)
{%- if node.description | length > 0 %} COMMENT = '{{ node.description }}'{% endif %}