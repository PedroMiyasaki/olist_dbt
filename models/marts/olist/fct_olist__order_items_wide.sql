with items as (
  select * from {{ ref('stg_olist__order_items') }}
),

orders as (
  select * from {{ ref('stg_olist__orders') }}
),

products as (
  select * from {{ ref('stg_olist__products') }}
),

sellers as (
  select * from {{ ref('stg_olist__sellers') }}
),

cat as (
  select * from {{ ref('stg_olist__category_translation') }}
),

seller_geo as (
  select * from {{ ref('int_olist__geo_by_zip') }}
)

select
  i.order_id,
  i.order_item_id,

  o.customer_id,
  o.order_status,
  o.order_purchase_ts,

  i.product_id,
  p.product_category_name,
  coalesce(cat.product_category_name_english, p.product_category_name) as product_category_name_en,

  i.seller_id,
  s.seller_city,
  s.seller_state,
  sg.avg_lat as seller_lat,
  sg.avg_lng as seller_lng,

  i.shipping_limit_ts,
  i.item_price,
  i.freight_value,
  (coalesce(i.item_price, 0) + coalesce(i.freight_value, 0)) as item_total_value

from items i
left join orders o
  on i.order_id = o.order_id
left join products p
  on i.product_id = p.product_id
left join cat
  on p.product_category_name = cat.product_category_name
left join sellers s
  on i.seller_id = s.seller_id
left join seller_geo sg
  on s.seller_zip_code_prefix = sg.zip_code_prefix
