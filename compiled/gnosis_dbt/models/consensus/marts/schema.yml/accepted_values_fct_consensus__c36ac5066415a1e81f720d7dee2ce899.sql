
    
    

with all_values as (

    select
        concentration_tier as value_field,
        count(*) as n_records

    from `dbt`.`fct_consensus_validators_withdrawal_addresses_distinct`
    group by concentration_tier

)

select *
from all_values
where value_field not in (
    'single','small (2-10)','medium (11-100)','large (101-1000)','whale (>1000)'
)


