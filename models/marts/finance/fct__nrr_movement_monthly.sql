{{ config(schema='marts', materialized='table', tags=['finance']) }}

-- NRR decomposition (new, expansion, contraction, churn) at the customer*month grain.

with snaps as (
  select
    customer_id,
    month_end,
    sum(mrr) as mrr_cust_month
  from {{ ref('int__subscription_monthly_snapshots') }}
  group by 1,2
),

lagged as (
  select
    customer_id,
    month_end,
    mrr_cust_month,
    lag(mrr_cust_month) over (partition by customer_id order by month_end) as prev_mrr
  from snaps
),

classified as (
  select
    month_end as period_end,
    customer_id,
    mrr_cust_month,
    prev_mrr,
    case when coalesce(prev_mrr,0)=0 and mrr_cust_month>0 then mrr_cust_month else 0 end as new_mrr,
    case when coalesce(prev_mrr,0)>0 and mrr_cust_month>prev_mrr then mrr_cust_month-prev_mrr else 0 end as expansion_mrr,
    case when coalesce(prev_mrr,0)>0 and mrr_cust_month<prev_mrr and mrr_cust_month>0 then prev_mrr-mrr_cust_month else 0 end as contraction_mrr,
    case when coalesce(prev_mrr,0)>0 and mrr_cust_month=0 then prev_mrr else 0 end as churn_mrr
  from lagged
),

-- Total MRR by month (rename month_end -> period_end here)
totals as (
  select
    month_end as period_end,
    sum(mrr_cust_month) as total_mrr
  from snaps
  group by 1
),

-- Prior-month total MRR (for the NRR ratio)
totals_with_prev as (
  select
    period_end,
    total_mrr,
    lag(total_mrr) over (order by period_end) as prev_total_mrr
  from totals
),

agg as (
  select
    c.period_end,
    t.total_mrr,
    sum(c.new_mrr)          as new_mrr,
    sum(c.expansion_mrr)    as expansion_mrr,
    sum(c.contraction_mrr)  as contraction_mrr,
    sum(c.churn_mrr)        as churn_mrr,
    /* Classic NRR ratio: current total MRR / prior-month total MRR */
    t.total_mrr / nullif(t.prev_total_mrr, 0) as nrr_ratio
  from classified c
  join totals_with_prev t
    on t.period_end = c.period_end
  group by 1,2,7
)

select * from agg
order by period_end
