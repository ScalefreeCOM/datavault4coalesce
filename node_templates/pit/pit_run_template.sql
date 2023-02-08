{% if config.preSQL %}
	{{ stage('Pre-SQL') }}
	{{ config.preSQL }}
{% endif %}

{{ stage('Insert New Rows') }}

INSERT INTO {{ ref_no_link(node.location.name, node.name) }}
WITH records_to_insert AS (
	SELECT
	{% for col in sources[0].columns -%}
		{%- if col.sourceColumns[0].column.is_hk -%}
			COALESCE({{ col.sourceColumns[0].node.name }}."{{ col.name }}", {{ datavault4coalesce.config.unknown_value__STRING }}) AS "{{ col.sourceColumns[0].node.name }}_{{ col.name }}"
		{%- elif col.sourceColumns[0].column.is_ldts -%}
			COALESCE({{ col.sourceColumns[0].node.name }}."{{ col.name }}", {{ datavault4coalesce__string_to_timestamp(datavault4coalesce.config.timestamp_format, datavault4coalesce.config.beginning_of_all_times) }}) AS "{{ col.sourceColumns[0].node.name }}_{{ col.name }}"
		{%- elif col.name == datavault4coalesce.config.sdts_alias -%}
			{{ col.sourceColumns[0].node.name }}."{{ col.name }}" AS "{{ col.name }}"
		{%- else -%}
			{{ col.sourceColumns[0].node.name }}."{{ col.name }}" AS "{{ col.sourceColumns[0].node.name }}_{{ col.name }}"
		{%- endif %}

		{%- if not loop.last -%} , {% endif %}
    {% endfor %}

	FROM "{{ storageLocations[0].database }}"."{{ sources[0].dependencies[0].node.location.name }}"."{{ sources[0].dependencies[0].node.name }}" {{ sources[0].dependencies[0].node.name }}

	{% for col in sources[0].columns -%}
		{%- if col.name == datavault4coalesce.config.sdts_alias -%}
			JOIN "{{ storageLocations[0].database }}"."{{ sources[0].dependencies[0].node.location.name }}"."{{ col.sourceColumns[0].node.name }}" {{ col.sourceColumns[0].node.name }}
			ON {{ col.sourceColumns[0].node.name }}."{{ datavault4coalesce.config.snapshot_trigger_column }}" = true
		{%- endif -%}
	{% endfor %}

	{%- for col in sources[0].columns -%}
		{%- if col.sourceColumns[0].column.is_hk and not loop.first -%}
			LEFT JOIN "{{ storageLocations[0].database }}"."{{ sources[0].dependencies[0].node.location.name }}"."{{ col.sourceColumns[0].node.name }}" {{ col.sourceColumns[0].node.name }}
			ON {{ sources[0].dependencies[0].node.name }}."{{ col.name }}" = {{ col.sourceColumns[0].node.name }}."{{ col.name }}"
			{% for sdts_col in sources[0].columns %}
				{%- if sdts_col.name == "sdts" -%}
					AND {{ sdts_col.sourceColumns[0].node.name }}."{{ sdts_col.name}}" BETWEEN {{ col.sourceColumns[0].node.name }}."{{ datavault4coalesce.config.ldts_alias }}" and {{ col.sourceColumns[0].node.name }}."{{ datavault4coalesce.config.ledts_alias }}"
				{%- endif -%}
			{%- endfor -%}
		{% endif %}
	{% endfor -%}
)

SELECT * FROM records_to_insert
WHERE "{{ datavault4coalesce.config.sdts_alias }}" NOT IN 
(
	SELECT {{ datavault4coalesce.config.sdts_alias }} FROM 
	{{ ref_no_link(node.location.name, node.name) }}
)


{% if config.cleanup_pit %}

{{ stage('Clean up PIT') }}

	DELETE FROM {{ ref_no_link(node.location.name, node.name) }}
	WHERE {{ datavault4coalesce.config.sdts_alias }} NOT IN (
		SELECT "{{ datavault4coalesce.config.sdts_alias }}" FROM 
			{% for col in sources[0].columns %}
				{%- if col.name == datavault4coalesce.config.sdts_alias -%}
					"{{ storageLocations[0].database }}"."{{ sources[0].dependencies[0].node.location.name }}"."{{ col.sourceColumns[0].node.name }}"
				{%- endif -%}
			{%- endfor %}
		WHERE "{{ datavault4coalesce.config.snapshot_trigger_column }}" = true
		)
{% endif %}

{% if config.postSQL %}
	{{ stage('Post-SQL') }}
	{{ config.postSQL }}
{% endif %}