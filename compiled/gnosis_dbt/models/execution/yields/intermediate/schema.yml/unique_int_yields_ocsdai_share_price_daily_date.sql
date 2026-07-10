
    
    

select
    date as unique_field,
    count(*) as n_records

from `dbt`.`int_yields_ocsdai_share_price_daily`
where date is not null
group by date
having count(*) > 1


