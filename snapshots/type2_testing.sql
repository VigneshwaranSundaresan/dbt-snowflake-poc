{% snapshot store_scd2_snapshot %}

{{
config(
      target_database='dbt_demo_db',
      target_schema='snapshots',
      unique_key='S_STORE_SK',
      strategy='timestamp',
      updated_at='VALID_FROM_DT',
      )
}}


select * from
{{ ref('scd_type2_xfm') }}

{% endsnapshot %}
