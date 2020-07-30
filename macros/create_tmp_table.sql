--
/*
    create a table relation
*/
{% macro create_temp_table_old(temp_relation ,sql) %}
    {%- set create_tmp_query -%}
          CREATE OR REPLACE TEMPORARY TABLE {{ temp_relation }}
          AS ( {{ sql.upper() }} )
          ;
    {%- endset -%}
    {% do run_query(create_tmp_query) %}
{% endmacro %}
--
{% macro create_temp_table(temp_relation ,sql) %}
          CREATE OR REPLACE TEMPORARY TABLE {{ temp_relation }}
          AS ( {{ sql.upper() }} )
          ;
{% endmacro %}
