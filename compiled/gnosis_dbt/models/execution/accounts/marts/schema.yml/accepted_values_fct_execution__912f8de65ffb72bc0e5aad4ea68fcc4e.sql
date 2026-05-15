
    
    

with all_values as (

    select
        address_type as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_account_profile_latest`
    group by address_type

)

select *
from all_values
where value_field not in (
    'safe','eoa_or_contract'
)


