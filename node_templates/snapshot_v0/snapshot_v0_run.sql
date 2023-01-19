{{ stage('Insert New Rows') }}

{%- set timestamp_format = parameters.datavault4coalesce__timestamp_format -%}
{%- set start_date = config.input_snapshot_start_date -%}
{%- set end_date = config.input_snapshot_end_date -%}
{%- set daily_snapshot_time = config.input_daily_snapshot_time -%}

INSERT INTO {{ ref_no_link(node.location.name, node.name) }}

WITH "date_base" AS (
    SELECT
        "sdts" as "{{ parameters.datavault4coalesce__sdts_alias }}",
        CONCAT('Snapshot ', DATE("sdts")) AS "caption",
        CASE
            WHEN EXTRACT(MINUTE FROM "sdts") = 0 AND EXTRACT(SECOND FROM "sdts") = 0 THEN TRUE
            ELSE FALSE
        END AS "is_hourly",
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM  "sdts") = 2 THEN TRUE
            ELSE FALSE
        END AS "is_beginning_of_week",
        CASE
            WHEN EXTRACT(DAY FROM "sdts") = 1 AND EXTRACT(MONTH FROM "sdts") in (1, 4, 7, 10) THEN TRUE
            ELSE FALSE
        END AS "is_beginning_of_month",
        CASE
            WHEN EXTRACT(DAY FROM "sdts") = 1 THEN TRUE
            ELSE FALSE
        END AS "is_beginning_of_quarter",  
        CASE
            WHEN EXTRACT(DAY FROM "sdts") = 1 AND EXTRACT(MONTH FROM "sdts") = 1 THEN TRUE
            ELSE FALSE
        END AS "is_beginning_of_year"
    FROM 
    (
        SELECT
            DATEADD(DAY, SEQ4(), 
            TIMESTAMPADD(SECOND, EXTRACT(SECOND FROM TO_TIME('{{ daily_snapshot_time }}')), 
            TIMESTAMPADD(MINUTE, EXTRACT(MINUTE FROM TO_TIME('{{ daily_snapshot_time }}')), 
            TIMESTAMPADD(HOUR, EXTRACT(HOUR FROM TO_TIME('{{ daily_snapshot_time }}')), TO_DATE('{{ start_date }}', 'YYYY-MM-DD')))
            ))::TIMESTAMP AS "sdts"
        FROM 
            TABLE(GENERATOR(ROWCOUNT => 100000))
        WHERE 
            "sdts" <= TO_DATE('{{ end_date }}', 'YYYY-MM-DD')
    ) 
),

"records_to_insert" AS (

    SELECT 
        "date_base".*
    FROM "date_base"
    LEFT JOIN {{ ref_no_link(node.location.name, node.name) }} "tgt"
        ON "date_base"."{{ parameters.datavault4coalesce__sdts_alias }}" = "tgt"."{{ parameters.datavault4coalesce__sdts_alias }}"
    WHERE "tgt"."{{ parameters.datavault4coalesce__sdts_alias }}" IS NULL

)

SELECT * FROM "records_to_insert"

{%- if node.description | length > 0 %} COMMENT = '{{ node.description }}'{% endif %}