with src as (
  select *
  from {{ source('orders', 'OLIST_ORDER_PAYMENTS_DATASET') }}
)

select
  order_id,
  payment_sequential::int      as payment_sequential,
  payment_type,
  payment_installments::int    as payment_installments,
  payment_value::float         as payment_value
from src
