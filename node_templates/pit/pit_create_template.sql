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


{# ABOVE ONE IS OLD VERSION, BELOW IS NEW WITHOUT "MULTI SOURCE" #}


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