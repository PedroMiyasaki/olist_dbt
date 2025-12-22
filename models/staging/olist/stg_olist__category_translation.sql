with src as (
  select *
  from {{ source('orders', 'PRODUCT_CATEGORY_NAME_TRANSLATION') }}
)

select
  c1 as product_category_name,
  c2 as product_category_name_english
from src
