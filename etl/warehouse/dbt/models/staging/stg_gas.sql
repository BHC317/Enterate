-- Gas simulado -> normaliza a modelo común
select
  md5(concat_ws('||',
      'gas','gas',
      coalesce(direccion,''),
      coalesce(nullif(start_ts,''),''),
      coalesce(nullif(end_ts,''),'')
  ))                                             as fingerprint,
  'gas'                                          as source,
  'gas'                                          as category,
  case
    when coalesce(nullif(lower(programado::text),''),'false')
         in ('true','t','1','yes','y','si','sí') then 'planned'
    else 'unplanned'
  end                                            as status,
  'Madrid'                                       as city,
  direccion                                      as street,
  null::text                                     as street_number,
  lat::double precision                          as lat,
  lon::double precision                          as lon,
  nullif(start_ts,'')::timestamptz               as start_ts_utc,
  nullif(end_ts,'')::timestamptz                 as end_ts_utc,
  mensaje                                        as description,
  event_id::text                                 as event_id,
  now() at time zone 'utc'                       as ingested_at_utc
from {{ source('staging','gas') }}
