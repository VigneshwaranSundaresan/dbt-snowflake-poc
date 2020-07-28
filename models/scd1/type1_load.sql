
{% set natural_key = dbt_utils.surrogate_key('S_STORE_ID','S_STORE_NAME') %}

{{
config(
materialized='incremental',
incremental_strategy='delete+insert',
alias='STORE_TYPE1_TESTING',
unique_key=natural_key
)
}}

select *,
to_date('{{ var('odate') }}') as AS_OF_DT
FROM {{ ref('store_prep_xfm') }}
