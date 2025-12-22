with src as (
  select *
  from {{ source('orders', 'OLIST_ORDER_REVIEWS_DATASET') }}
)

select
  review_id,
  order_id,
  review_score::int                           as review_score,
  review_comment_title,
  review_comment_message,
  try_to_timestamp_ntz(review_creation_date)  as review_creation_ts,
  try_to_timestamp_ntz(review_answer_timestamp) as review_answer_ts
from src
