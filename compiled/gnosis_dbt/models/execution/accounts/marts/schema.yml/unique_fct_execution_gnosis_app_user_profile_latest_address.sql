
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`fct_execution_gnosis_app_user_profile_latest`
where address is not null
group by address
having count(*) > 1


