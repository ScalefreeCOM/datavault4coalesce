
{#-- Utility macro: outputs a SQL function to create a hub table. --#}
{%- macro datavault4coalesce__databricks_hub_create(node, config, sources, columns) -%}


  {{ stage('Create Hub Table') }}

  CREATE OR REPLACE TABLE {{ ref_no_link(node.location.name, node.name) }}
  (
    {% for col in columns %}
      `{{ col.name }}` {{ col.dataType }}
      {%- if not col.nullable %} NOT NULL
        {%- if col.defaultValue | length > 0 %} DEFAULT {{ col.defaultValue }}{% endif %}
      {% endif %}
      {%- if col.description | length > 0 %} COMMENT '{{ col.description }}'{% endif %}
      {%- if not loop.last -%}, {% endif %}
    {% endfor %}
  )
  {%- if node.description | length > 0 %} COMMENT = '{{ node.description }}'{% endif %}
{% endmacro -%}