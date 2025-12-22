with orders as (
  select * from {{ ref('stg_olist__orders') }}
),

customers as (
  select * from {{ ref('stg_olist__customers') }}
),

items_agg as (
  select * from {{ ref('int_olist__order_items_agg') }}
),

payments_agg as (
  select * from {{ ref('int_olist__payments_agg') }}
),

reviews_agg as (
  select * from {{ ref('int_olist__reviews_agg') }}
),

cust_geo as (
  select * from {{ ref('int_olist__geo_by_zip') }}
),

join_customers as (
  select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.avg_lat as customer_lat,
    g.avg_lng as customer_lng
  from customers c
  left join cust_geo g
    on c.customer_zip_code_prefix = g.zip_code_prefix
)

select
  o.order_id,
  o.customer_id,

  jc.customer_unique_id,
  jc.customer_city,
  jc.customer_state,
  jc.customer_lat,
  jc.customer_lng,

  o.order_status,
  o.order_purchase_ts,
  o.order_approved_ts,
  o.order_delivered_carrier_ts,
  o.order_delivered_customer_ts,
  o.order_estimated_delivery_ts,

  -- Delivery analytics
  datediff('day', o.order_purchase_ts, o.order_delivered_customer_ts) as days_to_deliver,
  datediff('day', o.order_purchase_ts, o.order_estimated_delivery_ts) as days_to_estimated_delivery,
  case
    when o.order_delivered_customer_ts is null or o.order_estimated_delivery_ts is null then null
    when o.order_delivered_customer_ts <= o.order_estimated_delivery_ts then 1
    else 0
  end as delivered_on_time,

  -- Items
  ia.item_lines,
  ia.distinct_products,
  ia.distinct_sellers,
  ia.items_gmv,
  ia.freight_total,
  (coalesce(ia.items_gmv, 0) + coalesce(ia.freight_total, 0)) as order_gmv_with_freight,

  -- Payments
  pa.payment_total,
  pa.max_installments,
  pa.payment_rows,
  pa.distinct_payment_types,
  pa.paid_with_credit_card,
  pa.paid_with_boleto,

  -- Reviews
  ra.review_id,
  ra.review_score,
  ra.review_creation_ts,
  ra.review_answer_ts

from orders o
left join join_customers jc
  on o.customer_id = jc.customer_id
left join items_agg ia
  on o.order_id = ia.order_id
left join payments_agg pa
  on o.order_id = pa.order_id
left join reviews_agg ra
  on o.order_id = ra.order_id
