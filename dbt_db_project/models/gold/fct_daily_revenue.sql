{{ config(materialized='table') }}

-- the goal of this model is to simply calculate the daily order revenue

with lines as (
  select
    o.order_id,
    cast(o.order_ts as date) as order_date,
    x.productId              as product_id,
    x.quantity               as quantity
  from {{ source('silver','orders_silver') }} o
  lateral view explode(from_json(o.products_json, 'array<struct<productId:bigint,quantity:int>>')) e as x
),
priced as (
  select
    l.order_date, l.order_id, l.product_id, l.quantity,
    p.price,
    l.quantity * p.price as line_amount
  from lines l
  left join {{ source('silver','products_silver') }} p
    on p.product_id = l.product_id
)
select order_date, sum(line_amount) as revenue, sum(quantity) as units
from priced
group by order_date;

