-- Ayuntamiento (tráfico/obras)
select
  md5(concat_ws('||',
      'ayto',
      case when coalesce(es_obras,false) then 'road_works'
           when coalesce(es_accidente,false) then 'accident'
           else 'road' end,
      coalesce(municipio,''), coalesce(coalesce(via,descripcion,''),''), coalesce(numero,''),
      coalesce(nullif(start_ts,''),'') , coalesce(nullif(end_ts,''),'')
  ))                                           as fingerprint,
  'ayto'                                       as source,
  case when coalesce(es_obras,false) then 'road_works'
       when coalesce(es_accidente,false) then 'accident'
       else 'road' end                         as category,
  case when nullif(estado,'') = '1' then 'active' else 'planned' end      as status,
  municipio                                    as city,
  coalesce(via, descripcion)                   as street,
  nullif(numero,'')                            as street_number,
  lat::double precision                        as lat,
  lon::double precision                        as lon,
  nullif(start_ts,'')::timestamptz             as start_ts_utc,
  nullif(end_ts,'')::timestamptz               as end_ts_utc,
  descripcion                                  as description,
  coalesce(id_incidencia::text, event_id::text) as event_id,
  now() at time zone 'utc'                     as ingested_at_utc
from "appdb"."staging"."road"