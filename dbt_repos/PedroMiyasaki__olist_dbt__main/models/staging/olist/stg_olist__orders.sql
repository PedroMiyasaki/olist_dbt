select *
from {{ source('olist', 'orders') }}
where customer_id not like '%_DRIFT'