
{%- macro datavault4coalesce__snowflake_standardise_suffix(case_sensitive, hash_alg, is_hashdiff, datatype, zero_key, alias, multi_active_key, main_hashkey_col) -%}
{%- set dict_result = {} -%}
{%- set ldts_alias = datavault4coalesce.config.ldts_alias -%}
{%- set zero_key = datavault4coalesce__snowflake_as_constant(column_str=zero_key) -%}
{%- set listagg_closing = "" -%}

{#-- If definition of multi-active key(s) is found, prep the following string variables: --#}
{#--     multi_active_key: list of multi-active keys, concatenated with comma delimiter. --#}
{#--     listagg_closing : 2nd part of LISTAGG window function, used in                  --#}
{#--                       hashing calculation of multi-active                           --#}
{#--                       satellite's hash diff attribute.                              --#}
{%- if is_hashdiff and multi_active_key is defined and multi_active_key|length>0 -%}
    {%- set multi_active_key = multi_active_key|join('", "') -%}
    {%- set listagg_closing = ' WITHIN GROUP (ORDER BY "{}") OVER (PARTITION BY "{}", "{}"))'.format(multi_active_key, main_hashkey_col, ldts_alias) -%}
{%- endif -%}

{%- if 'VARCHAR' in datatype or 'CHAR' in datatype or 'STRING' in datatype or 'TEXT' in datatype %}
    {%- if case_sensitive -%}
        {%- if alias is not none -%}
    {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')){}, {}) AS {}".format(listagg_closing, zero_key, alias)-%}      
        {%- else -%}
    {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')){}, {})".format(listagg_closing, zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')){}, {}) AS {}".format(listagg_closing, zero_key, alias)-%}
        {%- else -%}
    {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')){}, {})".format(listagg_closing, zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- else -%}
    {%- if case_sensitive -%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')){}, {}) AS {}".format(listagg_closing, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')){}, {})".format(listagg_closing, zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')){}, {}) AS {}".format(listagg_closing, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')){}, {})".format(listagg_closing, zero_key)-%}
        {%- endif -%}
    {%- endif -%}
{%- endif -%}
{{ standardise_suffix }}
{%- endmacro -%}