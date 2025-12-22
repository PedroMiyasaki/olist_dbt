with src as (
  select *
  from {{ source('orders', 'OLIST_GEOLOCATION_DATASET') }}
)

select
  geolocation_zip_code_prefix,
  geolocation_lat,
  geolocation_lng,
  geolocation_city,
  geolocation_state
from src
