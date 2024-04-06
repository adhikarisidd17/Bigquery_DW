with prep as (
select
  age_bin,
  sum(monday) monday,
  sum(tuesday) tuesday,
  sum(wednesday) wednesday,
  sum(thursday) thursday,
  sum(friday) friday,
  sum(saturday) saturday,
  sum(sunday) sunday,
from
  `dbt_sidd.test_sample_data`
group by
  1
),

unpivoting  as (
select *
from prep 
unpivot (visits for weekday in (monday,tuesday,wednesday,thursday,friday,saturday,sunday))
)

select * from unpivoting 
qualify dense_rank() over (partition by age_bin order by visits desc) = 1 --Multiple days could be popular in each age group.
order by 1

