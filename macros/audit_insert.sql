{% macro audit_insert(model_name,
  model_status,
  model_exe_start_time,
  model_exe_end_time,
  insert_recs,
  del_recs,
  update_recs,
  status) %}

    INSERT INTO DBT_DEMO_DB.DBT_STG.AUDIT_TABLE
    SELECT max(NVL(audit_sk,0)) + 1 as audit_sk,
    '{{ model_name }}',
    {{ model_status }},
    {{ model_exe_start_time }},
    convert_timezone('America/Chicago', {{ model_exe_end_time }}::timestamp_ltz),
    {{insert_recs}},
    {{del_recs}},
    {{ update_recs }},
    {{ status }}
    FROM DBT_DEMO_DB.DBT_STG.AUDIT_TABLE;

{% endmacro %}
