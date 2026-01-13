with src as (
  select *
  from {{ source('orders', 'PRODUCT_CATEGORY_NAME_TRANSLATION') }}
)

select
  replace(replace(c1, 'fashio', 'fashion'), 'costruction', 'construction') as product_category_name_english,
  c2 as product_category_name
from src