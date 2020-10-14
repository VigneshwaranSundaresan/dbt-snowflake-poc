Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
# dbt-snowflake-poc

convert_timezone('UTC','US/Central',current_timestamp()::timestamp_ntz)


Audit Table Sql:

CREATE OR REPLACE TABLE DBT_DEMO_DB.DBT_STG.AUDIT_TABLE
( audit_sk BIGINT,
 model_name STRING,
    model_status STRING,
    model_exe_start_time timestamp_tz,
    model_exe_end_time timestamp_tz,
    insert_recs BIGINT,
    del_recs BIGINT,
    update_recs BIGINT,
    status STRING

);


grant create schema on database "DBT_DEMO_DB" to role DBT_ELT_RW;
grant usage on all schemas in database "DBT_DEMO_DB" to role DBT_ELT_RW;
grant usage on future schemas in database "DBT_DEMO_DB" to role DBT_ELT_RW;
grant select on all tables in database "DBT_DEMO_DB" to role DBT_ELT_RW;
grant select on future tables in database "DBT_DEMO_DB" to role DBT_ELT_RW;
grant select on all views in database "DBT_DEMO_DB" to role DBT_ELT_RW;
grant select on future views in database "DBT_DEMO_DB" to role DBT_ELT_RW;

run_commands:

Please run in the same order

Xfm Loads:

dbt run --m transformation.store_prep_xfm
dbt run --m transformation.data_prep_xfm1
dbt run --m transformation.data_prep_xfm2
dbt run --m transformation.data_join_xfm
dbt run --m transformation.scd_type2_xfm

SCD Load:


dbt run --m scd1.reusable_type1_load --vars '{"odate" : "2020-07-26"}'
dbt run --m scd1.reusable_del_insert --vars '{"odate" : "2020-07-26"}'
dbt run --m scd1.reusable_type1_trunc_load
dbt snapshot
dbt run --m scd2.final_scd2_load




dbt run --m scd1.type1_load --vars '{"odate" : "2020-07-26"}'
dbt run --m scd1.reusable_type1_load --vars '{"odate" : "2020-08-21"}'

AUDIT_SK	MODEL_NAME	MODEL_STATUS	MODEL_EXE_START_TIME	MODEL_EXE_END_TIME	INSERT_RECS	DEL_RECS	UPDATE_RECS	STATUS
1	STORE_M_TEST_FINAL	executing model	2020-07-30 13:53:57.286 -0400	2020-07-30 13:53:57.286 -0400	1500	1500	0	SUCCESS
2	STORE_M_TEST_FINAL	executing model	2020-07-30 13:55:12.878 -0400	2020-07-30 13:55:12.878 -0400	1500	1500	0	SUCCESS
