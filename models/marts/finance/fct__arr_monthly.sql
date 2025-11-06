{{ config(schema='marts', materialized='table', tags=['finance']) }}

with snaps as (
  select * from {{ ref('int__subscription_monthly_snapshots') }}
),
agg as (
  select
    month_end as period_end,
    sum(mrr) as total_mrr,
    12 * sum(mrr) as total_arr
  from snaps
  group by 1
)

select * from agg order by period_end
