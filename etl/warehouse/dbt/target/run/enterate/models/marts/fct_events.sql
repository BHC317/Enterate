
      
        
        
        delete from "appdb"."analytics_analytics"."fct_events" as DBT_INTERNAL_DEST
        where (fingerprint) in (
            select distinct fingerprint
            from "fct_events__dbt_tmp011008358313" as DBT_INTERNAL_SOURCE
        );

    

    insert into "appdb"."analytics_analytics"."fct_events" ("street", "lat", "lon", "start_ts_utc", "end_ts_utc", "description", "ingested_at_utc", "fingerprint", "source", "category", "status", "city", "street_number", "event_id")
    (
        select "street", "lat", "lon", "start_ts_utc", "end_ts_utc", "description", "ingested_at_utc", "fingerprint", "source", "category", "status", "city", "street_number", "event_id"
        from "fct_events__dbt_tmp011008358313"
    )
  