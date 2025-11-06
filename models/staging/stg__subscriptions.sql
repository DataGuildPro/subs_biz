with src as (
  select * from {{ source('raw_prod','subscriptions') }}
),

normalized as (
  select
    subscription_id,
    customer_id,
    plan_tier,
    billing_period,
    seat_count,
    unit_price,
    greatest(start_date, to_date('{{ var("start_date") }}')) as start_date,
    case
      when end_date is not null and end_date > to_date('{{ var("end_date") }}')
        then to_date('{{ var("end_date") }}')
      else end_date
    end as end_date,
    coalesce(end_date, '9999-12-31'::date) as effective_end_date,
    status,
    trial_start,
    trial_end,
    (seat_count * unit_price)::number(12,2) as mrr,
    (12 * seat_count * unit_price)::number(12,2) as arr,
    {{ surrogate_key(["subscription_id"]) }} as subscription_sk
  from src
  where start_date <= to_date('{{ var("end_date") }}')
    and (end_date is null or end_date >= to_date('{{ var("start_date") }}'))
)
select * from normalized
