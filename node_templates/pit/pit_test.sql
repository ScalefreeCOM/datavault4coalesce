{% if config.preSQL %}
	{{ stage('Pre-SQL') }}
	{{ config.preSQL }}
{% endif %}


{% for source in sources %}

SELECT
    te.{{ hashkey }},
    snap.{{ sdts }},
    {%- for source in sources %}
        COALESCE({{ satellite }}.{{ hashkey }}, CAST('{{ unknown_key }}' AS {{ hash_dtype }})) AS hk_{{ satellite }},
        COALESCE({{ satellite }}.{{ ldts }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) AS {{ ldts }}_{{ satellite }}
        {{- "," if not loop.last }}
    {%- endfor %}

FROM
    {{ ref(tracked_entity) }} te

{% endfor %}


{% if config.postSQL %}
	{{ stage('Post-SQL') }}
	{{ config.postSQL }}
{% endif %}