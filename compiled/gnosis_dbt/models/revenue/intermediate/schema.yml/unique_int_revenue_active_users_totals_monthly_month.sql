
    
    

select
    month as unique_field,
    count(*) as n_records

from `dbt`.`int_revenue_active_users_totals_monthly`
where month is not null
group by month
having count(*) > 1


