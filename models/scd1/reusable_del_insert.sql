{{
config(materialized='type1_trunc_load',
      date_col_name='as_of_dt',
      date_val=var('odate'),
      del_insert_flag=true,
      alias='STORE_M_TEST_DEL_INS')
}}

SELECT *,
to_date('{{ var('odate') }}') as AS_OF_DT
FROM {{ ref('data_join_xfm') }}
