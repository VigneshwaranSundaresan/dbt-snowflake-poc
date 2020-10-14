{% set natural_key = dbt_utils.surrogate_key('S_STORE_ID','S_STORE_NAME') %}

{{
config(
materialized='universal_type1_load',
alias='STORE_TYPE1_COMMON_TESTING',
unique_key=natural_key
)
}}

select *,
to_date('{{ var('odate') }}') as AS_OF_DT
FROM {{ ref('data_join_xfm') }}
