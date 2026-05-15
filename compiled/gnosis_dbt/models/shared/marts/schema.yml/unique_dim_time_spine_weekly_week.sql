
    
    

select
    week as unique_field,
    count(*) as n_records

from `dbt`.`dim_time_spine_weekly`
where week is not null
group by week
having count(*) > 1


