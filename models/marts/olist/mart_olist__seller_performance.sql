with i as (
  select *
  from {{ ref('fct_olist__order_items_wide') }}
)

select
  seller_id,
  seller_state,
  count(distinct order_id) as orders,
  count(*)                 as items_sold,
  sum(item_total_value)    as gmv,
  avg(item_total_value)    as avg_item_value
from i
group by 1,2
order by gmv desc
