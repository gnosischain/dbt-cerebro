
    
    

select
    user_pseudonym as unique_field,
    count(*) as n_records

from `dbt`.`fct_execution_gnosis_app_gt_user_identities_public`
where user_pseudonym is not null
group by user_pseudonym
having count(*) > 1


