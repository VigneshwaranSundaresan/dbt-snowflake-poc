{% macro audit_insert(model_name,
  model_status,
  model_exe_start_time,model_exe_end_time,
  insert_recs,del_recs,update_recs,status) %}

    INSERT INTO STAGE_DB.STAGE_SCHEMA.AUDIT_TABLE
    SELECT max(audit_sk) + 1 as audit_sk,
    model_name,
    model_status,
    model_exe_start_time,
    model_exe_end_time,
    insert_recs,
    del_recs,
    update_recs,
    status
    FROM INSERT INTO STAGE_DB.STAGE_SCHEMA.AUDIT_TABLE;

{% endmacro %}
