
    
    

with all_values as (

    select
        counterparty_class as value_field,
        count(*) as n_records

    from `dbt`.`int_celo_gpay_funder_wallet_transfers`
    group by counterparty_class

)

select *
from all_values
where value_field not in (
    'fee_sink','zero_address','gp_card','cardholder_wallet','other'
)


