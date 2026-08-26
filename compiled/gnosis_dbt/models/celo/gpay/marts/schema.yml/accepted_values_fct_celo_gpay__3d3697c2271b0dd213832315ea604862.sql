
    
    

with all_values as (

    select
        funding_channel as value_field,
        count(*) as n_records

    from `dbt`.`fct_celo_gpay_card_funding`
    group by funding_channel

)

select *
from all_values
where value_field not in (
    'cip64_direct_solo','other_direct','hub','mediated','mixed','unknown'
)


