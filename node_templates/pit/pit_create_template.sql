{{ stage('Create PIT Table') }}

{%- set ns = namespace(sdts_datatype = '') %}

CREATE OR REPLACE TABLE {{ ref_no_link(node.location.name, node.name) }}
(
    {% for source in sources %}
        {%- if loop.first %}
            {% for col in source.columns %}
                {%- if col.name == "sdts" or col.is_dimension_key or col.is_Hub_hk or col.is_Link_hk %}
                    {%- if col.name == "sdts" %}
                        {% set ns.sdts_datatype = col.dataType %}
                    {%- endif %}
                    {{ col.name }} {{ col.dataType }},
                {% endif %}
            {% endfor %}
        {% else %}
            {% for col in source.columns %}
                {% if col.is_Hub_hk or col.is_Link_hk %}
                    {{ source.columns[1].sourceColumns[0].node.name }}_{{ col.name }} {{ col.dataType }},
                    {{ source.columns[1].sourceColumns[0].node.name }}_{{ datavault4coalesce.config.ldts_alias }} {{ ns.sdts_datatype }}
                    
                {% endif %}
            {% endfor %}
            {%- if not loop.last -%}, {% endif %}
        {% endif %}
        
    {% endfor %}
)
{%- if node.description | length > 0 %} COMMENT = '{{ node.description }}'{% endif %}