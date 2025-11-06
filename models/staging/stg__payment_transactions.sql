{{ config(
    schema='staging',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='transaction_id'
) }}

with src as (
  select * from {{ source('raw_prod','payment_transactions') }}
),

filtered as (
  select
    transaction_id,
    subscription_id,
    customer_id,
    invoice_id,
    transaction_date,
    period_start,
    period_end,
    amount,
    currency,
    tax_amount,
    discount_amount,
    status,
    payment_method,
    created_at,
    updated_at
  from src
  {% if is_incremental() %}
    -- Only bring in new/changed rows
    where updated_at > (select coalesce(max(updated_at),'1900-01-01') from {{ this }})
  {% endif %}
)

select * from filtered
