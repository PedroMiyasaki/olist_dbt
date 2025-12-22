with r as (
  select *
  from {{ ref('stg_olist__reviews') }}
),

dedup as (
  select
    *,
    row_number() over (
      partition by order_id
      order by review_creation_ts desc nulls last, review_id desc
    ) as rn
  from r
)

select
  order_id,
  review_id,
  review_score,
  review_creation_ts,
  review_answer_ts
from dedup
where rn = 1
