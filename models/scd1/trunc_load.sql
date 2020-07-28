{{
config(
materialized='incremental',
transient=false,
incremental_strategy='delete+insert',
pre_hook='delete from {{this}}',
alias='CUSTOMER_TOTAL_TRUNC_LOAD'
)
}}

SELECT *,
to_date('{{ var('odate') }}') as AS_OF_DT
FROM {{ ref('data_prep_xfm2') }}
