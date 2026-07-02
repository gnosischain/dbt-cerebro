
    
    

select
    week as unique_field,
    count(*) as n_records

from `dbt`.`fct_revenue_active_users_totals_weekly`
where week is not null
group by week
having count(*) > 1


