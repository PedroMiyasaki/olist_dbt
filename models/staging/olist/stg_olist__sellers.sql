with src as (
  select *
  from {{ source('orders', 'OLIST_SELLERS_DATASET') }}
)

select
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
from src
