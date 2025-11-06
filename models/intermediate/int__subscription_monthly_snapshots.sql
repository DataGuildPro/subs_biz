{{ config(schema='intermediate', materialized='table') }}

with subs as (
  select * from {{ ref('stg__subscriptions') }}
),
months as (
  select distinct month_start
  from {{ ref('dim__calendar') }}
),
expanded as (
  -- one row per subscription per active month
  select
    s.subscription_id,
    s.customer_id,
    s.plan_tier,
    s.billing_period,
    s.seat_count,
    s.unit_price,
    s.mrr,
    s.arr,
    m.month_start,
    last_day(m.month_start) as month_end
  from subs s
  join months m
    on m.month_start between date_trunc('month', s.start_date)
                         and date_trunc('month', s.effective_end_date)
),

final as (
  select
    subscription_id,
    customer_id,
    plan_tier,
    billing_period,
    seat_count,
    unit_price,
    mrr,
    arr,
    month_start,
    month_end
  from expanded
)

select * from final
