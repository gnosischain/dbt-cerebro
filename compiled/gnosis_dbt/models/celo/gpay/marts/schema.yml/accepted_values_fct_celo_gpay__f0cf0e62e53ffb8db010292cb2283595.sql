
    
    

with all_values as (

    select
        destination as value_field,
        count(*) as n_records

    from `dbt`.`fct_celo_gpay_wallet_share_of_spend_daily`
    group by destination

)

select *
from all_values
where value_field not in (
    'to_card','to_elsewhere'
)


