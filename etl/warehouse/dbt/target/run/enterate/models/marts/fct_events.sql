
      
        
        
        delete from "appdb"."analytics_analytics"."fct_events" as DBT_INTERNAL_DEST
        where (fingerprint) in (
            select distinct fingerprint
            from "fct_events__dbt_tmp003839153110" as DBT_INTERNAL_SOURCE
        );

    

    insert into "appdb"."analytics_analytics"."fct_events" ("fingerprint", "source", "category", "status", "city", "street", "street_number", "lat", "lon", "start_ts_utc", "end_ts_utc", "description", "event_id", "ingested_at_utc")
    (
        select "fingerprint", "source", "category", "status", "city", "street", "street_number", "lat", "lon", "start_ts_utc", "end_ts_utc", "description", "event_id", "ingested_at_utc"
        from "fct_events__dbt_tmp003839153110"
    )
  