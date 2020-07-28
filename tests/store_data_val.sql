select a.cnt from (
  select count(*) as cnt,S_STORE_SK
  from {{ ref('store_prep_xfm') }}
  group by S_STORE_SK
  having S_STORE_SK > 1
) a
where a.cnt > 1
