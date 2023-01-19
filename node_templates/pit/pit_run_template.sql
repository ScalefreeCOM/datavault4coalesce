{% if config.preSQL %}
	{{ stage('Pre-SQL') }}
	{{ config.preSQL }}
{% endif %}

{{ stage('Insert New Rows') }}

INSERT INTO {{ ref_no_link(node.location.name, node.name) }}
WITH records_to_insert AS (
	SELECT
	{% for source in sources %}
		{% if loop.first %}
			{% for col in source.columns if col.is_dimension_key or col.is_Hub_hk or col.is_Link_hk %}
				h.{{ col.name }},
			{% endfor %}
			{% for col in source.columns if col.is_system_sdts %}
				{{ get_source_transform(col) }} as {{ col.name }},
			{% endfor %}
		{% endif %}
		{% if not loop.first %}
			{% for col in source.columns %}
				{% if col.is_Hub_hk or col.is_Link_hk %}
					COALESCE( "{{ source.columns[1].sourceColumns[0].node.name }}"."{{ col.name }}", {{ datavault4coalesce__unknown_key() }} ) as {{ source.columns[1].sourceColumns[0].node.name }}_{{ col.name }},
				{% elif col.name == parameters.datavault4coalesce__ldts_alias %}
					COALESCE( "{{ source.columns[1].sourceColumns[0].node.name }}"."{{ col.name }}", '{{ parameters.datavault4coalesce__beginning_of_all_times }}' ) as {{ source.columns[1].sourceColumns[0].node.name }}_{{ col.name }},
				{% endif %}
			{% endfor %}
		{% endif %}
		{% if loop.last %}
			{% for col in source.columns if col.is_system_sdts %}
				{{ col.name }}
			{% endfor %}
		{% endif %}
	{% endfor %}

	FROM "{{ sources[0].dependencies[0].node.location.name }}"."{{ sources[0].dependencies[0].node.name }}" h

	{% for source in sources %}
		{% if not loop.first %}
			JOIN "{{ source.columns[1].sourceColumns[0].node.location.name }}"."{{ source.columns[1].sourceColumns[0].node.name }}" {{ source.columns[1].sourceColumns[0].node.name }}
			ON h.{{ sources[0].columns[0].name }} = {{ source.columns[1].sourceColumns[0].node.name }}.{{ source.columns[0].name }}
			AND 
			{% for col in columns if col.is_system_sdts %}
				{{ col.name }}
			{% endfor %}
			BETWEEN {{ source.columns[1].sourceColumns[0].node.name }}.{{ parameters.datavault4coalesce__ldts_alias }} AND {{ source.columns[1].sourceColumns[0].node.name }}.{{ parameters.datavault4coalesce__ledts_alias }}
		{% endif %}
	{% endfor %}
)

SELECT * FROM records_to_insert

{% if config.postSQL %}
	{{ stage('Post-SQL') }}
	{{ config.postSQL }}
{% endif %}