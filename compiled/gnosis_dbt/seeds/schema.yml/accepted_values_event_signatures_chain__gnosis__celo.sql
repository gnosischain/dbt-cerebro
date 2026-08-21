
    
    

with all_values as (

    select
        chain as value_field,
        count(*) as n_records

    from `dbt`.`event_signatures`
    group by chain

)

select *
from all_values
where value_field not in (
    'gnosis','celo'
)


