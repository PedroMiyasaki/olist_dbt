with p as (
  select *
  from {{ ref('stg_olist__payments') }}
)

select
  order_id,
  sum(payment_value)                                  as payment_total,
  max(payment_installments)                           as max_installments,
  count(*)                                            as payment_rows,
  count(distinct payment_type)                        as distinct_payment_types,
  max(case when payment_type = 'credit_card' then 1 else 0 end) as paid_with_credit_card,
  max(case when payment_type = 'boleto' then 1 else 0 end)      as paid_with_boleto
from p
group by 1
