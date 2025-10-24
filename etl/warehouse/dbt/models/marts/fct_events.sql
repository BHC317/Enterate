{{ config(materialized='incremental', unique_key='fingerprint') }}

with unioned as (
  select * from {{ ref('stg_electricity') }}
  union all
  select * from {{ ref('stg_water') }}
  union all
  select * from {{ ref('stg_road') }}
  union all
  select * from {{ ref('stg_gas') }}
)

select * from unioned

{% if is_incremental() %}
where ingested_at_utc >
  (select coalesce(max(ingested_at_utc),'1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
