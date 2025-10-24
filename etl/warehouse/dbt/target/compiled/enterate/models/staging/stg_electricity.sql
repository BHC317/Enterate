-- i-DE (electricidad) => columnas esperadas por el modelo común
select
  md5(concat_ws('||',
      'ide','electricity', coalesce(municipio,''), coalesce(via,''), coalesce(numero,''),
      (to_timestamp(fecha||' '||hora_inicio,'DD/MM/YYYY HH24:MI') at time zone 'Europe/Madrid')::text,
      (to_timestamp(fecha||' '||hora_fin   ,'DD/MM/YYYY HH24:MI') at time zone 'Europe/Madrid')::text
  ))                                           as fingerprint,
  'ide'                                        as source,
  'electricity'                                as category,
  'planned'                                    as status,
  municipio                                    as city,
  via                                          as street,
  nullif(numero,'')                            as street_number,
  lat::double precision                        as lat,
  lon::double precision                        as lon,
  (to_timestamp(fecha||' '||hora_inicio,'DD/MM/YYYY HH24:MI') at time zone 'Europe/Madrid') as start_ts_utc,
  (to_timestamp(fecha||' '||hora_fin   ,'DD/MM/YYYY HH24:MI') at time zone 'Europe/Madrid') as end_ts_utc,
  null::text                                   as description,
  null::text                                   as event_id,
  now() at time zone 'utc'                     as ingested_at_utc
from "appdb"."staging"."electricity"