with f as (
  select *
  from {{ ref('fct_olist__order_wide') }}
  where order_purchase_ts is not null
)

select
  date_trunc('day', order_purchase_ts) as order_day,
  count(*)                             as orders,
  sum(order_gmv_with_freight)          as gmv,
  avg(order_gmv_with_freight)          as aov,
  avg(delivered_on_time)               as pct_on_time,
  avg(review_score)                    as avg_review_score
from f
group by 1
order by 1
