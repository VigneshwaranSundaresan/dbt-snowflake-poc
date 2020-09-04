{% materialization universal_type2_load, default %}

    {%- set natural_keys = config.get('natural_keys',default='none') -%}
    {%- set target_table_name = model['alias'] -%}
    {%- set model_start_time = run_started_at.astimezone(modules.pytz.timezone("America/Chicago")) -%}
    --
    {{ log("Unique Key is/are : " ~ natural_keys) }}
    {{ log("Target Table Name : " ~ target_table_name) }}
    {{ log("Model Start Time : " ~ model_start_time) }}
    --
    {% set model_name = this.name %}
    {{ log("Model name after sub :" ~ model_name ) }}
    --
    {% if natural_keys is none %}
      {% do exceptions.warn("Unique key cannot be empty for SCD Type 2") %}
      {%- call statement('main',fetch_result=false) -%}
        {{ audit_insert(model_name,
                        "'executing model'",
                        model_start_time,
                        current_timestamp(),
                        0,
                        0,
                        0,
                        "'FAILED'")}}
      {%- endcall -%}
    {% endif %}
    --
    {%- set current_table = adapter.get_relation(database=database,
                                                    schema=schema,
                                                    identifier=target_table_name) -%}
    --
    {%- set stg_tmp_identifier = target_table_name + 'STAGE_TMP' -%}
    {%- set stg_tmp_table = api.Relation.create(database=database,
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
    {%- call statement('copy',fetch_result=false) -%}
        {{ create_temp_table(stg_tmp_table, sql) }}
    {%- endcall -%}
    -- Drop temp table
    {{ adapter.drop_relation(stg_tmp_table) }}
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
