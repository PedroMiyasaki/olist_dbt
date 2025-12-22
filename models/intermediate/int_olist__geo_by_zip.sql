with geo as (
  select *
  from {{ ref('stg_olist__geolocation') }}
)

select
  geolocation_zip_code_prefix as zip_code_prefix,
  avg(geolocation_lat)        as avg_lat,
  avg(geolocation_lng)        as avg_lng,
  max(geolocation_city)       as city,
  max(geolocation_state)      as state
from geo
group by 1
