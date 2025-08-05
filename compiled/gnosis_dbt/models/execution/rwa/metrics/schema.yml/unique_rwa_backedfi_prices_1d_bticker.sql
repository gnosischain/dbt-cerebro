
    
    

select
    bticker as unique_field,
    count(*) as n_records

from `dbt`.`rwa_backedfi_prices_1d`
where bticker is not null
group by bticker
having count(*) > 1


