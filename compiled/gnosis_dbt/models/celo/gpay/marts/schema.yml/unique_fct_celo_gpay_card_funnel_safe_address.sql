
    
    

select
    safe_address as unique_field,
    count(*) as n_records

from `dbt`.`fct_celo_gpay_card_funnel`
where safe_address is not null
group by safe_address
having count(*) > 1


