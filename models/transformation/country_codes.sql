{{
config(materialized='table',
      transient=true,
      schema='stg')
}}

select * from {{ ref('country') }}
