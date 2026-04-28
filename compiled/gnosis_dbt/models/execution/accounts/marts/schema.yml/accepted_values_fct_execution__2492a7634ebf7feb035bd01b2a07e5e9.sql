
    
    

with all_values as (

    select
        direction as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_account_token_movements_daily`
    group by direction

)

select *
from all_values
where value_field not in (
    'in','out'
)


