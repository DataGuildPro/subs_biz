{% snapshot subscriptions_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='subscription_id',
    strategy='check',
    check_cols=['plan_tier','billing_period','seat_count','unit_price']
  )
}}

select
  subscription_id,
  customer_id,
  plan_tier,
  billing_period,
  seat_count,
  unit_price,
  start_date,
  end_date,
  status,
  trial_start,
  trial_end
from {{ source('raw_prod','subscriptions') }}

{% endsnapshot %}
