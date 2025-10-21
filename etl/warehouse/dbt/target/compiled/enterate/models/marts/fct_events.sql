

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

