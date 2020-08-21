{{
config(materialized='type1_trunc_load',
      trunc_load_flag=true,
      alias='STORE_M_TEST_FINAL')
}}

SELECT *
FROM DBT_DEMO_DB.DBT_STG.STORE_SAMPLE_STG
