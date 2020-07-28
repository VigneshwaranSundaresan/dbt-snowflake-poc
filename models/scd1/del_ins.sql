
{{
config(
materialized='incremental',
transient=false,
incremental_strategy='delete+insert',
alias='CUSTOMER_TOTAL_ORDERS_DEL_INS',
unique_key='as_of_dt'
)
}}

SELECT *,
to_date('{{ var('odate') }}') as AS_OF_DT
FROM {{ ref('data_join_xfm') }}
