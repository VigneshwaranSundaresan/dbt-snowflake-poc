/*
Thi smaterialization is to load the type 1 tables. The materialization currently supports:
1) Truncate and load
2) Delete for a particular date and load the data
3) Regular scd type 1 tables where history is ot maintained
*/
{% materialization type1_elt_load_old, default %}

    {% set unique_key = config.get('unique_key',default='none') %}
    {%- set date_col = config.get('date_col_name',default='none') -%}
    {%- set date_value = config.get('date_val',default='none') -%} /* added for delete and insert */
    {%- set del_insert_flag = config.get('del_insert_flag',default='none') -%}
    {%- set trunc_load_flag = config.get('trunc_load_flag',default='none') -%}
    {%- set target_table_name = model['alias'] -%}
    --
    {%- set current_table = adapter.get_relation(database=database,
                                                    schema=schema,
                                                    identifier=target_table_name) -%}
    --
    {%- set tmp_identifier = target_table_name + '_TMP' -%}
    {%- set tmp_table = api.Relation.create(database=database,
                                           schema=schema,
                                           identifier=tmp_identifier,
                                           type='table') -%}
    --
    {%- set current_relation_exists_as_table = (current_table is not none and current_table.is_table) -%}
    --
    {{ log("Current Relation type/status : " ~ current_relation_exists_as_table) }}
    --setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
    -- Raise exception if the table is missing
    {% if not current_table %}
      {% do exceptions.warn("Unable to find the relation/table") %}
    {% endif %}
    -- Call for trunc and load
    {% if trunc_load_flag or del_insert_flag %}
            {{ log("Calling Delete SQL ") }}
            {{ type1_del(current_table,date_col,date_value,trunc_load_flag,del_insert_flag) }}
    {% endif %}
    -- load the temp table
    {{ log("Temp table name is : " ~ tmp_table) }}
    {{ create_temp_table(tmp_table, sql) }}
    {{ log("Temp table Created successfully") }}
    /*
    get columns from temp tables
    */
    {% set tmp_columns = adapter.get_columns_in_relation(tmp_table) %}
    {% set target_columns = adapter.get_columns_in_relation(current_table) %}
    {%- set temp_cols_csv = tmp_columns | map(attribute="name") | join(', ') -%}
    {%- set target_cols_csv = target_columns | map(attribute="name") | join(', ') -%}
    --
    {{ log("Temp Column Names are :" ~ temp_cols_csv) }}
    {{ log("Target Column Names are :" ~ target_cols_csv) }}
    /*
    -- execute sql to insert into the table
    */
    {{ type1_insert(current_table,tmp_table,target_cols_csv,temp_cols_csv) }}
    --
    {% set model_name = this.name %}
    {{ log("Model name after sub :" ~ model_name ) }}
    --
    {%- call statement('main',fetch_result=false) -%}
      {{ audit_insert(model_name,
                      "'executing model'",
                      current_timestamp(),
                      current_timestamp(),
                      0,
                      0,
                      0,
                      "'SUCCESS'")}}
    {%- endcall -%}

    -- Drop temp table
    {{ adapter.drop_relation(tmp_table) }}
    --
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    -- `COMMIT` happens here
    {% do adapter.commit() %}
    --
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    --
    {{ log("Post hooks executed successfully ") }}
    --
{% endmaterialization %}
