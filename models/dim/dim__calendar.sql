{{ config(schema='dim', materialized='table') }}

with bounds as (
  select
    to_date('{{ var("start_date") }}') as start_d,
    to_date('{{ var("end_date") }}')   as end_d
),
span as (
  select
    start_d,
    end_d,
    datediff(day, start_d, end_d) + 1 as days_incl
  from bounds
),
gen as (
  -- constant upper bound; filter later
  select row_number() over (order by seq4()) as n
  from table(generator(rowcount => 1000))
),
day_spine as (
  select
    dateadd(day, n - 1, start_d) as date_day
  from gen
  cross join span
  where n <= days_incl
),
final as (
  select
    date_day,
    to_char(date_day, 'YYYY-MM') as yyyymm,
    date_trunc('month', date_day) as month_start,
    last_day(date_day) as month_end,
    extract(year from date_day)::int as year_num,
    extract(month from date_day)::int as month_num,
    case when date_day = last_day(date_day) then 1 else 0 end as is_month_end
  from day_spine
)

select * from final
