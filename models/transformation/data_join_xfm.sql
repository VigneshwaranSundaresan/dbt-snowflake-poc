{{
config(materialized='table',
      alias='CUSTOMER_TOTAL_ORDERS_STG',
      transient=true,
      schema='stg')
}}

select c.C_CUSTKEY,
        c.C_NAME,
        c.C_NATIONKEY,
        o.TOTALPRICE,
        --CASE WHEN c.C_ACCTBAL >= o.TOTALPRICE THEN 'POSITIVE'
        --                                      ELSE 'NEGATIVE'
        --END AS STATUS
        {{ nation_key_val('c.C_NATIONKEY') }} REGION
from {{ ref('data_prep_xfm1') }} c
LEFT JOIN  {{ ref('data_prep_xfm2') }} o
ON c.C_CUSTKEY = o.O_CUSTKEY
where o.TOTALPRICE > {{ var('default_null_variable') }}
