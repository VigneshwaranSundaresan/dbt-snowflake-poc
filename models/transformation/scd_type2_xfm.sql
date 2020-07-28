
{{
config(materialized='table',
      unique_key='S_STORE_SK',
      schema='stg',
      alias='STORE_SAMPLE_TYPE2_STG',
      transient=true)
}}

select S_STORE_SK,
S_STORE_ID,
S_REC_START_DATE AS VALID_FROM_DT,
S_STORE_NAME,
S_NUMBER_EMPLOYEES
from {{ source('snowflake_sample1', 'store') }}
