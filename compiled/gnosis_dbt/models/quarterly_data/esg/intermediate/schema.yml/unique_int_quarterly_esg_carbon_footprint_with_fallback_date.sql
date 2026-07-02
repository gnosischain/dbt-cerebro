
    
    

select
    date as unique_field,
    count(*) as n_records

from `dbt`.`int_quarterly_esg_carbon_footprint_with_fallback`
where date is not null
group by date
having count(*) > 1


