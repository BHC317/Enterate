

with unioned as (
  select * from "appdb"."analytics_staging"."stg_electricity"
  union all
  select * from "appdb"."analytics_staging"."stg_water"
  union all
  select * from "appdb"."analytics_staging"."stg_road"
  union all
  select * from "appdb"."analytics_staging"."stg_gas"
)

select * from unioned


where ingested_at_utc >
  (select coalesce(max(ingested_at_utc),'1970-01-01T00:00:00Z') from "appdb"."analytics_analytics"."fct_events")
