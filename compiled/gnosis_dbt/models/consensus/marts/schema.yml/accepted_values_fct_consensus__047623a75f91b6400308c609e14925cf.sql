
    
    

with all_values as (

    select
        role as value_field,
        count(*) as n_records

    from `dbt`.`fct_consensus_consolidations_daily`
    group by role

)

select *
from all_values
where value_field not in (
    'self','source','target'
)


