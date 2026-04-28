
    
    

with all_values as (

    select
        direction as value_field,
        count(*) as n_records

    from `dbt`.`int_execution_account_token_movements_in_daily`
    group by direction

)

select *
from all_values
where value_field not in (
    'in'
)


