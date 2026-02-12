
    
    

with all_values as (

    select
        cohort_unit as value_field,
        count(*) as n_records

    from `dbt`.`int_execution_tokens_balance_cohorts_daily`
    group by cohort_unit

)

select *
from all_values
where value_field not in (
    'usd','native'
)


