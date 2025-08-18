
    
    

select
    bticker as unique_field,
    count(*) as n_records

from `dbt`.`api_execution_rwa_backedfi_prices_daily`
where bticker is not null
group by bticker
having count(*) > 1


