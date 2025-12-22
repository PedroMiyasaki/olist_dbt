with src as (
  select *
  from {{ source('orders', 'OLIST_ORDER_ITEMS_DATASET') }}
)

select
  order_id,
  order_item_id,
  product_id,
  seller_id,
  try_to_timestamp_ntz(shipping_limit_date) as shipping_limit_ts,
  price::float                              as item_price,
  freight_value::float                      as freight_value
from src
