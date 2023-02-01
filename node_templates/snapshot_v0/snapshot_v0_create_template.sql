{{ stage('Create Snapshot v0 Table') }}

CREATE OR REPLACE TABLE {{ ref_no_link(node.location.name, node.name) }}
(
    "{{ datavault4coalesce.config.sdts_alias }}" TIMESTAMP,
    "force_active" BOOLEAN,
    "replacement_sdts" TIMESTAMP,
    "caption" STRING,
    "is_hourly" BOOLEAN,
    "is_daily" BOOLEAN,
    "is_beginning_of_week" BOOLEAN,
    "is_beginning_of_month" BOOLEAN,
    "is_beginning_of_quarter" BOOLEAN,
    "is_beginning_of_year" BOOLEAN
)