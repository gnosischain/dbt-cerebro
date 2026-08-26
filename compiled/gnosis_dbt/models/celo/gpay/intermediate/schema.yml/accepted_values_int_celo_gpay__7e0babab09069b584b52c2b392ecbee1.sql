
    
    

with all_values as (

    select
        direction as value_field,
        count(*) as n_records

    from `dbt`.`int_celo_gpay_funder_wallet_transfers`
    group by direction

)

select *
from all_values
where value_field not in (
    'in','out'
)


