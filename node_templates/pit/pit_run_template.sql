{% if config.preSQL %}
	{{ stage('Pre-SQL') }}
	{{ config.preSQL }}
{% endif %}

{{ stage('Insert New Rows') }}

INSERT INTO {{ ref_no_link(node.location.name, node.name) }}
WITH records_to_insert AS (
	SELECT
	{% for col in sources[0].columns %}
		{{ col.sourceColumns[0].node.name }}."{{ col.name }}" AS "{{ col.sourceColumns[0].node.name }}_{{ col.name }}"
        {% if not loop.last %} , {% endif %}
    {% endfor %}

	FROM "{{ storageLocations[0].database }}"."{{ sources[0].dependencies[0].node.location.name }}"."{{ sources[0].dependencies[0].node.name }}" {{ sources[0].dependencies[0].node.name }}

	{% for col in sources[0].columns %}
		{% if col.name == "sdts" %}
			FULL OUTER JOIN "{{ storageLocations[0].database }}"."{{ sources[0].dependencies[0].node.location.name }}"."{{ col.sourceColumns[0].node.name }}" {{ col.sourceColumns[0].node.name }}
			ON {{ col.sourceColumns[0].node.name }}."is_active" = true
		{% endif %}
	{% endfor %}

	{% for col in sources[0].columns %}
		{% if col.sourceColumns[0].column.is_hk and not loop.first %}
			JOIN "{{ storageLocations[0].database }}"."{{ sources[0].dependencies[0].node.location.name }}"."{{ col.sourceColumns[0].node.name }}" {{ col.sourceColumns[0].node.name }}
			ON {{ sources[0].dependencies[0].node.name }}."{{ col.name }}" = {{ col.sourceColumns[0].node.name }}."{{ col.name }}"
			{% for sdts_col in sources[0].columns %}
				{% if sdts_col.name == "sdts" %}
					AND {{ sdts_col.sourceColumns[0].node.name }}."{{ sdts_col.name}}"
					BETWEEN {{ col.sourceColumns[0].node.name }}."ldts" and {{ col.sourceColumns[0].node.name }}."ledts"
				{% endif %}
			{% endfor %}
		{% endif %}
	{% endfor %}
)

SELECT * FROM records_to_insert

{% if config.postSQL %}
	{{ stage('Post-SQL') }}
	{{ config.postSQL }}
{% endif %}