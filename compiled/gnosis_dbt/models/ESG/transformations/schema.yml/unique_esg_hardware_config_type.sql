
    
    

select
    type as unique_field,
    count(*) as n_records

from `dbt`.`esg_hardware_config`
where type is not null
group by type
having count(*) > 1


