
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`int_crawlers_data_labels_dex`
where address is not null
group by address
having count(*) > 1


