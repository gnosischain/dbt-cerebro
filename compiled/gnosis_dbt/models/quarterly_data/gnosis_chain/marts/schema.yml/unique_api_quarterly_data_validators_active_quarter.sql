
    
    

select
    quarter as unique_field,
    count(*) as n_records

from `dbt`.`api_quarterly_data_validators_active`
where quarter is not null
group by quarter
having count(*) > 1


