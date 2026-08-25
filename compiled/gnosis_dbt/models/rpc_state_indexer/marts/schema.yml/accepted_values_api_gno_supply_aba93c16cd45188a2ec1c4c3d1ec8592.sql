
    
    

with all_values as (

    select
        label as value_field,
        count(*) as n_records

    from `dbt`.`api_gno_supply_daily`
    group by label

)

select *
from all_values
where value_field not in (
    'Ethereum Circ. Supply','Gnosis Circ. Supply','Non-Circ. Supply'
)


