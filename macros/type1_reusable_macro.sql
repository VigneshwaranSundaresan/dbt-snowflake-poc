/*
This macro does only DELETE
*/
{% macro type1_del(target_table_name,
                        date_col_name,
                        date_val,
                        trunc_load_flag,
                        del_insert_flag)
                        %}

    {{ log('del_insert_flag :' ~ del_insert_flag) }}
    {{ log('date_col_name :' ~ date_col_name) }}
    {{ log('date value :' ~ date_val) }}
    {{ log('trunc_load_flag :' ~ trunc_load_flag) }}
    {{ log('Target Table is :' ~ target_table_name) }}

    {% set del_query %}
        DELETE FROM {{ target_table_name }}
        {% if del_insert_flag is none %}
            WHERE {{ date_col_name }} = '{{ date_val }}'
        {% endif %}
    {% endset %}

    {{ log('Delete Query  :' ~ del_query) }}

    {% set results = run_query(del_query) %}

    {% if execute %}
      {# Return the first column #}
      {% set results_list = results.columns[0].values() %}
    {% else %}
      {% set results_list = [] %}
    {% endif %}

    {{ log('Rows Deleted  :' ~ results_list[0]) }}

{% endmacro %}
/*
Insert for Trunc and Load, Snapshot loade
*/
{% macro type1_insert_test(target_table_name,
                      temp_table_name,
                      target_cols,
                      temp_cols)
                      %}
                      
    {{ log('Target Table Name is :' ~ target_table_name) }}
    {{ log('Temp Table Name is :' ~ temp_table_name) }}

    {%- set insert_query -%}
        INSERT INTO {{ target_table_name }} ( {{target_cols}} )
        SELECT {{ temp_cols }}
        FROM {{ temp_table_name }}
        ;
    {%- endset -%}

    {{ log('Insert Query  :' ~ insert_query) }}

    {% set results = run_query(insert_query) %}

    {% if execute %}
      {# Return the first column #}
      {% set results_list = results.columns[0].values() %}
    {% else %}
      {% set results_list = [] %}
    {% endif %}

    {{ log('Rows Inserted  :' ~ results_list[0]) }}

{% endmacro %}
--
{% macro type1_insert(target_table_name,
                      temp_table_name,
                      target_cols,
                      temp_cols)
                      %}

    {{ log('Target Table Name is :' ~ target_table_name) }}
    {{ log('Temp Table Name is :' ~ temp_table_name) }}

    INSERT INTO {{ target_table_name }} ( {{target_cols}} )
    SELECT {{ temp_cols }}
    FROM {{ temp_table_name }}
    ;

{% endmacro %}
