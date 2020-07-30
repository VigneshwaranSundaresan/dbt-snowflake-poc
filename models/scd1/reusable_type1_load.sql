{{
config(materialized='type1_elt_load',
      date_col_name='as_of_dt',
      date_val=var('odate'),
      trunc_load_flag=true,
      alias='STORE_M_TEST_FINAL')
}}

SELECT *
FROM DBT_DEMO_DB.DBT_STG.STORE_SAMPLE_STG
