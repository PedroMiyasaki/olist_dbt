with items as (
  select *
  from {{ ref('stg_olist__order_items') }}
)

select
  order_id,
  count(*)                              as item_lines,
  count(distinct product_id)            as distinct_products,
  count(distinct seller_id)             as distinct_sellers,
  sum(item_price)                       as items_gmv,
  sum(freight_value)                    as freight_total,
  min(shipping_limit_ts)                as first_shipping_limit_ts,
  max(shipping_limit_ts)                as last_shipping_limit_ts
from items
group by 1
