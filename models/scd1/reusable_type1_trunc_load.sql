{{
config(materialized='type1_trunc_load',
      trunc_load_flag=true,
      alias='STORE_M_TEST_FINAL')
}}

SELECT *
FROM {{ ref('data_prep_xfm2') }}
