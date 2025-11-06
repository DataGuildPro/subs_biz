{{ config(schema='staging') }}

with src as (
  select * from {{ source('raw_prod','customers') }}
)

select
  customer_id,
  customer_uuid,
  company_name,
  industry,
  country,
  region,
  lead_source,
  currency,
  signup_date,
  is_enterprise,
  org_size_bucket,
  {{ surrogate_key(["customer_id"]) }} as customer_sk
from src
