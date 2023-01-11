{{ stage('Create Hub Table') }}

CREATE OR REPLACE TABLE {{ ref_no_link(node.location.name, node.name) }}
(
	{% for col in columns if col.is_Hub_hk or col.is_Hub_bk or col.is_Hub_ldts or col.is_Hub_rsrc %}

    {{ col.name }} {{ col.dataType }}

    {%- if not loop.last -%}, {% endif %}
    
  {% endfor %}
)