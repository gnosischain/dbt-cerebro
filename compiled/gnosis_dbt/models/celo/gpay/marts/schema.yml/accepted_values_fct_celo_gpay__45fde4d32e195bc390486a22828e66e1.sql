
    
    

with all_values as (

    select
        first_fund_channel as value_field,
        count(*) as n_records

    from (select * from `dbt`.`fct_celo_gpay_card_funnel` where first_fund_channel IS NOT NULL) dbt_subquery
    group by first_fund_channel

)

select *
from all_values
where value_field not in (
    'cip64_direct_solo','other_direct','hub','mediated','mixed','unknown'
)


