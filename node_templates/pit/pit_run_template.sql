{% if config.preSQL %}
	{{ stage('Pre-SQL') }}
	{{ config.preSQL }}
{% endif %}

{{ stage('Create PIT Table') }}

WITH records_to_insert AS (
	SELECT
	{% for source in sources %}
		{% if loop.first %}
			{% for col in source.columns %}
				{% if not col.name == "sdts" %}
					h.{{ col.name }},
				{% endif %}
			{% endfor %}
		{% endif %}
		{% if not loop.first %}
			{% for col in source.columns %}
				{% if col.name != "sdts" and col.name != "ldts" %}
					{% set src = get_source_transform(col).split('.') %}
					COALESCE( {{ sources[0].dependencies[0].node.name }}."{{ col.name }}", {{ parameters.datavault4coalesce__unknown_value__STRING }} as {{ src[0]|replace('"', '') }}_{{ col.name }} ),
				{% elif col.name == "ldts" %}
					{% set src = get_source_transform(col).split('.') %}
					COALESCE( {{ sources[0].dependencies[0].node.name }}."{{ col.name }}", "{{ parameters.datavault4coalesce__beginning_of_all_times }}" as {{ src[0]|replace('"', '') }}_{{ col.name }} ),
				{% endif %}
			{% endfor %}
		{% endif %}
		{% if loop.last %}
			{% for col in source.columns if col.name == "sdts" %}
				{{ col.name }}
			{% endfor %}
		{% endif %}
	{% endfor %}

	FROM "{{ sources[0].dependencies[0].node.location.name }}"."{{ sources[0].dependencies[0].node.name }}" h

	{% for source in sources %}

)


{% if config.postSQL %}
	{{ stage('Post-SQL') }}
	{{ config.postSQL }}
{% endif %}