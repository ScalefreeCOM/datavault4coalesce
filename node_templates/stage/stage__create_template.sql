{% if node.override.create.enabled %}
	
	{{ node.override.create.script }}

{% elif node.materializationType == 'table' %}
	{{ stage('Create Stage Table') }}

	CREATE OR REPLACE TABLE {{ ref_no_link(node.location.name, node.name) }}
	(
		{% for col in columns %}
			"{{ col.name }}" {{ col.dataType }}
			{%- if not col.nullable %} NOT NULL
				{%- if col.defaultValue | length > 0 %} DEFAULT {{ col.defaultValue }}{% endif %}
			{% endif %}
			{%- if col.description | length > 0 %} COMMENT '{{ col.description }}'{% endif %}
			{%- if not loop.last -%}, {% endif %}
		{% endfor %}
	)
	{%- if node.description | length > 0 %} COMMENT = '{{ node.description }}'{% endif %}


{% elif node.materializationType == 'view' %}
    {{ stage('Create Stage View') }}

    CREATE OR REPLACE VIEW {{ ref_no_link(node.location.name, node.name) }}
    (
        {% for col in columns %}
            {%- if col.is_rsrc_column -%}
                "{{datavault4coalesce.config.rsrc_alias}}"
            {%- elif col.is_ldts_column -%}
                "{{datavault4coalesce.config.ldts_alias}}"
            {% else %}
                "{{ col.name }}"
            {% endif %}
            {%- if col.description | length > 0 %} COMMENT '{{ col.description }}'{% endif %}
            {%- if not loop.last -%}, {% endif %}
        {% endfor %}

    )
    {%- if node.description | length > 0 %} COMMENT = '{{ node.description }}'{% endif %}
    AS
    {% for source in sources %}
        SELECT
        {% for col in source.columns %}
            {{ get_source_transform(col) }} AS "{{ col.name }}"
            {%- if not loop.last -%}, {% endif %}
        {% endfor %}

        {{ source.join }}

        {% if not loop.last %}
            {% if config.insertStrategy in ['UNION', 'UNION ALL'] %}
                {{ config.insertStrategy }}
            {% else %}
                UNION
            {% endif %}
        {% endif %}

        {%- if config.generate_ghost_records -%}

            UNION ALL 

            SELECT

            {% for source in sources %}
                {% for col in source.columns %}
                    {%- if "hashDetails" in col.keys() %}
                        {{ datavault4coalesce__ghost_record_per_datatype(col.name, col.dataType, 'unknown', hash=true) }}
                    {%- else -%}
                        {{ datavault4coalesce__ghost_record_per_datatype(col.name, col.dataType, 'unknown', hash=false) }}
                    {%- endif -%}
                {% if not loop.last %},{% endif %}
                {% endfor %}
            {% endfor %}

            UNION ALL 

            SELECT

            {% for source in sources %}
                {% for col in source.columns %}
                    {%- if "hashDetails" in col.keys() %}
                        {{ datavault4coalesce__ghost_record_per_datatype(col.name, col.dataType, 'error', hash=true) }}
                    {%- else -%}
                        {{ datavault4coalesce__ghost_record_per_datatype(col.name, col.dataType, 'error', hash=false) }}
                    {%- endif -%}
                {% if not loop.last %},{% endif %}
                {% endfor %}
            {% endfor %}

        {%- endif -%}

    {% endfor %}

{% endif %}