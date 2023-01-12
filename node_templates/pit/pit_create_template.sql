{{ stage('Create PIT Table') }}

CREATE OR REPLACE TABLE {{ ref_no_link(node.location.name, node.name) }}
(
    {% for source in sources %}
        {% for col in source.columns %}
            {% if not col.name == "sdts" %}
                {% set src = get_source_transform(col).split('.') %}
                {{ src[0]|replace('"', '') }}_{{ col.name }} {{ col.dataType }}
                {%- if not loop.last -%}, {% endif %}
            {% endif %}
		{% endfor %}
        {%- if loop.last %}
            {% for col in source.columns %}
                {%- if col.name == "sdts" %}
                    {{ col.name }} {{ col.dataType }}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
)
{%- if node.description | length > 0 %} COMMENT = '{{ node.description }}'{% endif %}