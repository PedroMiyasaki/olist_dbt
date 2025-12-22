with src as (
  select *
  from {{ source('orders', 'OLIST_ORDERS_DATASET') }}
)

select
  order_id,
  customer_id,
  order_status,

  try_to_timestamp_ntz(order_purchase_timestamp)          as order_purchase_ts,
  try_to_timestamp_ntz(order_approved_at)                 as order_approved_ts,
  try_to_timestamp_ntz(order_delivered_carrier_date)      as order_delivered_carrier_ts,
  try_to_timestamp_ntz(order_delivered_customer_date)     as order_delivered_customer_ts,
  try_to_timestamp_ntz(order_estimated_delivery_date)     as order_estimated_delivery_ts

from src
