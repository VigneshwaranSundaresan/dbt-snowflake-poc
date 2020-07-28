{{
config(materialized='table',
      transient=true,
      alias='CUSTOMER_EXTRACT_STG',
      schema='stg')
}}

select C_CUSTKEY,
        C_NAME,
        C_NATIONKEY
from {{ source('snowflake_sample', 'customer') }}
