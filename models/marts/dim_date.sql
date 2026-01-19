{{ config(materialized='table') }}

with date_bounds as (

  select
    least(
      (select min(sale_date) from {{ ref('stg_sales') }}),
      (select min(stock_date) from {{ ref('stg_stock') }})
    ) as min_date,
    greatest(
      (select max(sale_date) from {{ ref('stg_sales') }}),
      (select max(stock_date) from {{ ref('stg_stock') }})
    ) as max_date

),

date_spine as (

  select
    d as date_day
  from date_bounds,
  unnest(generate_date_array(min_date, max_date)) as d

)

select
  -- surrogate-like date key (stable and BI-friendly)
  cast(format_date('%Y%m%d', date_day) as int64) as date_key,

  date_day,
  extract(year from date_day) as year,
  extract(quarter from date_day) as quarter,
  extract(month from date_day) as month,
  format_date('%B', date_day) as month_name,
  extract(day from date_day) as day,

  extract(week from date_day) as week_of_year,
  extract(isoweek from date_day) as iso_week_of_year,

  extract(dayofweek from date_day) as day_of_week,
  format_date('%A', date_day) as day_name,

  case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,

  date_trunc(date_day, month) as month_start_date,
  date_trunc(date_day, quarter) as quarter_start_date,
  date_trunc(date_day, year) as year_start_date

from date_spine
order by date_day
