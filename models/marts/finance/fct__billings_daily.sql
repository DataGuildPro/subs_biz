{{ config(
    schema='marts',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='billing_day',
    tags=['finance']
) }}

with tx as (
  select * from {{ ref('stg__payment_transactions') }}
  where status in ('paid','refunded')  -- include refunds to get net cash
),

daily as (
  select
    transaction_date as billing_day,
    sum(amount) as net_billings_amount,             -- refunds negative
    sum(case when amount>0 then amount else 0 end) as gross_billings_amount,
    sum(case when amount<0 then -amount else 0 end) as refunds_amount
  from tx
  {% if is_incremental() %}
    where transaction_date > (select coalesce(max(billing_day),'1900-01-01') from {{ this }})
  {% endif %}
  group by 1
)

select * from daily
