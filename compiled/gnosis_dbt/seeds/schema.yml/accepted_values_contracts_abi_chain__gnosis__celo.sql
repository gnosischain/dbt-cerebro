
    
    

with all_values as (

    select
        chain as value_field,
        count(*) as n_records

    from `dbt`.`contracts_abi`
    group by chain

)

select *
from all_values
where value_field not in (
    'gnosis','celo'
)


