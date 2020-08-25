{% materialization universal_type1_load, default %}

    {%- set unique_key = config.get('unique_key',default='none') -%}
    {%- set target_table_name = model['alias'] -%}
    {%- set model_start_time = run_started_at.astimezone(modules.pytz.timezone("America/Chicago")) -%}
    --
    {{ log("Unique Key is/are : " ~ unique_key) }}
    {{ log("Target Table Name : " ~ target_table_name) }}
    {{ log("Model Start Time : " ~ model_start_time) }}
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
    {{ log("Current Relation type/status : " ~ current_relation_exists_as_table) }}
    --setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
    -- Raise exception if the table is missing
    {% if not current_table %}
      {% do exceptions.warn("Unable to find the relation/table") %}
    {% endif %}
    -- load the temp table
    {{ log("Temp table name is : " ~ tmp_table) }}
    {%- call statement('copy',fetch_result=false) -%}
        {{ create_temp_table(tmp_table, sql) }}
    {%- endcall -%}
    {{ log("Temp table Created successfully") }}
    -- compare the final table and tmp tables
    /*
    get columns from temp tables
    */
    {% set tmp_columns = adapter.get_columns_in_relation(tmp_table) %}
    {{ log("Just Checking :" ~ tmp_columns) }}
    {% set target_columns = adapter.get_columns_in_relation(current_table) %}
    {%- set temp_cols_csv = tmp_columns | map(attribute="name") | join(', ') -%}
    {%- set target_cols_csv = target_columns | map(attribute="name") | join(', ') -%}
    {{ log("Temp Column Names are :" ~ temp_cols_csv) }}
    {{ log("Target Column Names are :" ~ target_cols_csv) }}
    --
    {%- call statement('delete',fetch_result=true) -%}
        {{ universal_type1_delete(target_table_name,tmp_table,unique_key) }}
    {%- endcall -%}
    {%- set results = load_result('delete') -%}
    {%- set delete_count = results['status'].split(" ")[1] | int -%}
    {{ log("Delete count :" ~ delete_count) }}
    --
    /*
    -- execute sql to insert into the table
    */
    {%- call statement('insert',fetch_result=true) -%}
        {{ type1_insert(target_table_name,tmp_table,target_cols_csv,temp_cols_csv) }}
    {%- endcall -%}
    {%- set results = load_result('insert') -%}
    {%- set insert_count = results['status'].split(" ")[1] | int -%}
    {{ log("insert count :" ~ insert_count) }}
    --
    {% set model_name = this.name %}
    {{ log("Model name after sub :" ~ model_name ) }}
    --
    {%- call statement('main',fetch_result=false) -%}
      {{ audit_insert(model_name,
                      "'executing model'",
                      model_start_time,
                      current_timestamp(),
                      insert_count,
                      delete_count,
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
