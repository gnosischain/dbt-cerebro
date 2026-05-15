
    
    

select
    month as unique_field,
    count(*) as n_records

from `dbt`.`dim_time_spine_monthly`
where month is not null
group by month
having count(*) > 1


