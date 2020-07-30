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
