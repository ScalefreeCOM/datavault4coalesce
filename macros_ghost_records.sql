{%- macro datavault4coalesce__string_to_timestamp(format, timestamp) -%}
    TO_TIMESTAMP('{{ timestamp }}', '{{ format }}')
{%- endmacro -%}


{%- macro datavault4coalesce__is_expression(obj) -%}

     {%- if obj is string -%}
          {%- if (obj|first == "'" and obj|last == "'") or ("(" in obj and ")" in obj) or "::" in obj or "||" in obj -%}
               {{ true }}
          {%- else -%}
               {{ false }}
          {%- endif -%}
     {%- else -%}
          {{ false }}
     {%- endif -%}

{%- endmacro -%}


{%- macro datavault4coalesce__escape_column_name(column) -%}

     {%- set escape_char_left  = '"' -%}
     {%- set escape_char_right = '"' -%}

     {%- set escaped_column_name = escape_char_left ~ column|upper|replace(escape_char_left, '')|replace(escape_char_right, '')|trim ~ escape_char_right|indent(4) -%}

     {{ escaped_column_name }}

{%- endmacro -%}


{%- macro datavault4coalesce__as_constant(column_str) -%}

     {%- if column_str is not none and column_str is string and column_str -%}
          {%- if column_str|first == "!" -%}
               {{- "'" ~ column_str[1:] ~ "'" -}}
          {%- else -%}
               {%- if datavault4coalesce__is_expression(column_str) -%}
                    {{- column_str -}}
               {%- else -%}
                    {{- datavault4coalesce__escape_column_names(column_str) -}}
               {%- endif -%}
          {%- endif -%}
     {%- endif -%}
{%- endmacro -%}


{%- macro datavault4coalesce__ghost_record_per_datatype(column_name, datatype, ghost_record_type, hash) -%}

{%- set beginning_of_all_times = parameters.datavault4coalesce__beginning_of_all_times -%}
{%- set end_of_all_times = parameters.datavault4coalesce__end_of_all_times -%}
{%- set timestamp_format = parameters.datavault4coalesce__timestamp_format -%}
{%- set unknown_value__STRING = parameters.datavault4coalesce__unknown_value__STRING -%}
{%- set unknown_value_alt__STRING = parameters.datavault4coalesce__unknown_value_alt__STRING -%}
{%- set error_value__STRING = parameters.datavault4coalesce__error_value__STRING -%}
{%- set error_value_alt__STRING = parameters.datavault4coalesce__error_value_alt__STRING -%}

{%- set hash_datatype = parameters.datavault4coalesce__hash_datatype -%}

{%- set hash_alg = datavault4coalesce__hash_algorithm() -%}
{%- set unknown_key = datavault4coalesce__unknown_key() -%}
{%- set error_key = datavault4coalesce__error_key() -%}

{{ unknown_key }}

{%- if hash %}

     {%- if ghost_record_type == 'unknown' -%}
          {{ unknown_key }} as {{ column_name }}
     {%- elif ghost_record_type == 'error' -%}
          {{ error_key }} as {{ column_name }}
     {% endif %}

{% else %}

     {%- if ghost_record_type == 'unknown' -%}
     {%- if datatype in ['TIMESTAMP_NTZ','TIMESTAMP', 'DATE'] -%}{{ datavault4coalesce__string_to_timestamp(timestamp_format, beginning_of_all_times) }} AS {{ column_name }}
     {% elif datatype.upper().startswith('STRING') or datatype.upper().startswith('VARCHAR') %}
          {%- if datatype.upper().startswith('VARCHAR') and datatype[8:-1]|int >= unknown_value__STRING|length -%} {{unknown_value__STRING}} AS {{ column_name }}
          {%- elif datatype.upper().startswith('STRING') -%} {{unknown_value__STRING}} AS {{ column_name }}
          {% else %} {{unknown_value_alt__STRING}} AS {{ column_name }}
          {% endif %}
     {% elif datatype in ['NUMBER','INT','FLOAT','DECIMAL'] or datatype.upper().startswith('NUMBER') %}0 AS {{ column_name }}
     {% elif datatype == 'BOOLEAN' %}CAST('FALSE' AS BOOLEAN) AS {{ column_name }}
     {% else %}NULL AS {{ column_name }}
     {% endif %}
     {%- elif ghost_record_type == 'error' -%}
     {%- if datatype in ['TIMESTAMP_NTZ','TIMESTAMP', 'DATE'] -%}{{ datavault4coalesce__string_to_timestamp(timestamp_format, end_of_all_times) }} AS {{ column_name }}
     {% elif datatype.upper().startswith('STRING') or datatype.upper().startswith('VARCHAR') %}
          {%- if datatype.upper().startswith('VARCHAR') and datatype[8:-1]|int >= error_value__STRING|length -%} {{error_value__STRING}} AS {{ column_name }}
          {%- elif datatype.upper().startswith('STRING') -%} {{error_value__STRING}} AS {{ column_name }}
          {% else %} {{error_value_alt__STRING}} AS {{ column_name }}
          {% endif %}
     {% elif datatype in ['NUMBER','INT','FLOAT','DECIMAL'] or datatype.upper().startswith('NUMBER') %}-1 AS {{ column_name }}
     {% elif datatype == 'BOOLEAN' %}CAST('FALSE' AS BOOLEAN) AS {{ column_name }}
     {% else %}NULL AS {{ column_name }}
     {% endif %}
     {%- endif -%}

{%- endif -%}

{%- endmacro -%}


{%- macro datavault4coalesce__hash_algorithm() -%}

     {%- set hash_function = parameters.datavault4coalesce__hash -%}
     {%- set hash_datatype = parameters.datavault4coalesce__hash_datatype -%}

     {%- set dict_result = {} -%}
     {%- set hash_alg = '' -%}

     {%- if hash_function == 'MD5' and hash_datatype == 'STRING' -%}
          {%- set hash_alg = 'MD5' -%}
     {%- elif hash_function == 'SHA1' or hash_function == 'SHA1_HEX' or hash_function == 'SHA' -%} 
          {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
               {%- set hash_alg = 'SHA1' -%}
          {%- elif 'BINARY' in hash_datatype -%}
               {%- set hash_alg = 'SHA1_BINARY' -%}       
          {%- endif -%}
     {%- elif hash_function == 'SHA2' or hash_function == 'SHA2_HEX' -%}
          {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
               {%- set hash_alg = 'SHA2' -%}
          {%- elif 'BINARY' in hash_datatype -%}
               {%- set hash_alg = 'SHA2_BINARY' -%}       
          {%- endif -%}   
     {%- endif -%}

     {{ hash_alg }}

{%- endmacro -%}


{%- macro datavault4coalesce__unknown_key() -%}
     {%- set hash_function = parameters.datavault4coalesce__hash -%}
     {%- set hash_datatype = parameters.datavault4coalesce__hash_datatype -%}

     {%- set dict_result = {} -%}
     {%- set unknown_key = '' -%}

     {%- if hash_function == 'MD5' and hash_datatype == 'STRING' -%}
          {%- set unknown_key = '!00000000000000000000000000000000' -%}
     {%- elif hash_function == 'SHA1' or hash_function == 'SHA1_HEX' or hash_function == 'SHA' -%} 
          {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
               {%- set unknown_key = '!0000000000000000000000000000000000000000' -%}
          {%- elif 'BINARY' in hash_datatype -%}
               {%- set unknown_key = "TO_BINARY('0000000000000000000000000000000000000000')" -%}     
          {%- endif -%}
     {%- elif hash_function == 'SHA2' or hash_function == 'SHA2_HEX' -%}
          {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
               {%- set unknown_key = '!0000000000000000000000000000000000000000000000000000000000000000' -%}
          {%- elif 'BINARY' in hash_datatype -%}
               {%- set unknown_key = "TO_BINARY('0000000000000000000000000000000000000000000000000000000000000000')" -%}  
          {%- endif -%}   
     {%- endif -%}

     {% set unknown_key = datavault4coalesce__as_constant(unknown_key) %}
     {{ unknown_key }}

{%- endmacro -%}


{%- macro datavault4coalesce__error_key() -%}
     {%- set hash_function = parameters.datavault4coalesce__hash -%}
     {%- set hash_datatype = parameters.datavault4coalesce__hash_datatype -%}

     {%- set dict_result = {} -%}
     {%- set error_key = '' -%}

     {%- if hash_function == 'MD5' and hash_datatype == 'STRING' -%}
          {%- set error_key = '!ffffffffffffffffffffffffffffffff' -%}
     {%- elif hash_function == 'SHA1' or hash_function == 'SHA1_HEX' or hash_function == 'SHA' -%} 
          {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
               {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffff' -%}
          {%- elif 'BINARY' in hash_datatype -%}
               {%- set error_key = "TO_BINARY('ffffffffffffffffffffffffffffffffffffffff')" -%}        
          {%- endif -%}
     {%- elif hash_function == 'SHA2' or hash_function == 'SHA2_HEX' -%}
          {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
               {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
          {%- elif 'BINARY' in hash_datatype -%}
               {%- set error_key = "TO_BINARY('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')" -%}        
          {%- endif -%}   
     {%- endif -%}

     {% set error_key = datavault4coalesce__as_constant(error_key) %}
     {{ error_key }}

{%- endmacro -%}