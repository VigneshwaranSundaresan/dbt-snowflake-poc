

{{
config(materialized='table',
      alias='STORE_DIM_TESTING')
}}

select S_STORE_SK,
S_STORE_ID,
S_STORE_NAME,
S_NUMBER_EMPLOYEES,
VALID_FROM_DT,
CASE
    WHEN DBT_VALID_TO IS NULL THEN '9999-12-31'
                               ELSE DBT_VALID_TO
END AS VALID_TO_DT,
CASE
    WHEN DBT_VALID_TO IS NULL THEN 'Y'
                               ELSE 'N'
END AS CURRENT_REC_IND
FROM {{ ref('store_scd2_snapshot') }}
