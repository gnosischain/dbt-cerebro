
    
    

select
    bticker as unique_field,
    count(*) as n_records

from `dbt`.`int_execution_rwa_backedfi_prices`
where bticker is not null
group by bticker
having count(*) > 1


