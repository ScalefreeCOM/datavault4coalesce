{% if config.preSQL %}
	{{ stage('Pre-SQL') }}
	{{ config.preSQL }}
{% endif %}

{% for source in sources %}

	{{ stage('Merge Hub - ' ~ source.name) }}
	MERGE INTO {{ ref_no_link(node.location.name, node.name) }} "TGT" USING
	(
		SELECT DISTINCT
		{% for col in source.columns %}
			{{ get_source_transform(col) }} AS "{{ col.name }}"
			{%- if not loop.last -%}, {% endif %}
		{% endfor %}

		{{ source.join }}

	)
	AS "SRC"
	ON
	{% for col in sources[0].columns if (col.is_Hub_hk) -%}
		{% if not loop.first %}
			AND
		{% endif %}
		"SRC"."{{ col.name }}" = "TGT"."{{ col.name }}"
	{% endfor %}
	WHEN NOT MATCHED THEN
	INSERT
	(
		{% for col in columns %}
			"{{ col.name }}"
			{%- if not loop.last -%}, {% endif %}
		{% endfor %}
	) VALUES
	(
		{% for col in columns %}
			"SRC"."{{ col.name }}"
			{%- if not loop.last -%}, {% endif %}
		{% endfor %}
	)

{% endfor %}

{% if config.postSQL %}
	{{ stage('Post-SQL') }}
	{{ config.postSQL }}
{% endif %}