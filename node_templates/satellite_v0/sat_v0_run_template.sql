{% if config.preSQL %}
	{{ stage('Pre-SQL') }}
	{{ config.preSQL }}
{% endif %}

{{ stage('Insert New Rows') }}
INSERT INTO {{ ref_no_link(node.location.name, node.name) }}
WITH latest_entries_in_sat AS (
	/* get current rows from satellite */
	{% for col in columns if col.is_hk or col.is_hd %}
		{%- if loop.first -%}SELECT {% endif %} 
		"{{col.name}}"
		{%- if not loop.last -%}, {% endif %}
		{%- if loop.last %} 
			FROM {{ ref_no_link(node.location.name, node.name) }} 
			QUALIFY ROW_NUMBER() OVER (PARTITION BY "{{ get_value_by_column_attribute("is_hk") }}" ORDER BY "{{ parameters.datavault4coalesce__ldts_alias }}" DESC) = 1
		{% endif %}
	{% endfor %}
),

deduplicated_numbered_source AS (
    
    {% for source in sources %}

        SELECT
		{% for col in source.columns %}
			{{ get_source_transform(col) }} AS {{ col.name }},
		{% endfor %}
        ROW_NUMBER() OVER(PARTITION BY "{{ get_value_by_column_attribute("is_hk") }}" ORDER BY "{{ parameters.datavault4coalesce__ldts_alias }}") as rn
        
        {{ source.join }}
        QUALIFY
        CASE
            WHEN "{{ get_value_by_column_attribute("is_hd") }}" = LAG("{{ get_value_by_column_attribute("is_hd") }}") OVER(PARTITION BY "{{ get_value_by_column_attribute("is_hk") }}" ORDER BY "{{ parameters.datavault4coalesce__ldts_alias }}" ) THEN FALSE
            ELSE TRUE
        END

    {% endfor %}

)

	{% for source in sources %}
		SELECT DISTINCT
		{% for col in source.columns %}
			{{ col.name }}
			{%- if not loop.last -%}, {% endif %}
		{% endfor %}

		FROM deduplicated_numbered_source
	WHERE NOT EXISTS (
		SELECT 1 FROM latest_entries_in_sat
		WHERE 
		{% for col in source.columns if col.is_hk or col.is_hd %}
			{% if not loop.first %}
				AND
			{% endif %}
			deduplicated_numbered_source.{{ col.name }} = latest_entries_in_sat."{{ col.name }}"
		{% endfor %}
        AND deduplicated_numbered_source.rn = 1
	)

	{% endfor %}

{% if config.postSQL %}
	{{ stage('Post-SQL') }}
	{{ config.postSQL }}
{% endif %}