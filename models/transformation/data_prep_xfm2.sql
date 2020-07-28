{{
config(materialized='table',
      transient=true,
      alias='CUST_ORDER_TOTAL_PRICE_STG',
      schema='stg')
}}

SELECT O_CUSTKEY,
      sum(O_TOTALPRICE) as TOTALPRICE
--FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS"
FROM {{ source('snowflake_sample', 'orders') }}
GROUP BY O_CUSTKEY
