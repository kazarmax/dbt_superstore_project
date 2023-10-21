with recursive date_cte as (
    select dateadd(day, 0, '2000-01-01') as date
    union all
    select dateadd(day, 1, date)
    from date_cte
    where date <= '2030-01-01'
)

select
    date::date as date,
    to_char(date, 'yyyymmdd')::int as date_id,
    extract(year from date)::int as year,
    extract(quarter from date)::int as quarter,
    extract(month from date)::int as month,
    extract(week from date)::int as week,
    dayofweekiso(date)::int as dow,
    dayname(date) as week_day
from date_cte