{{
config(materialized='table',
      alias='STORE_SAMPLE_STG',
      transient=true,
      schema='stg')
}}

select S_STORE_SK,
S_STORE_ID,
S_REC_START_DATE,
S_REC_END_DATE,
S_STORE_NAME,
S_NUMBER_EMPLOYEES
from {{ source('snowflake_sample1', 'store') }}
