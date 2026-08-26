
    
    

with all_values as (

    select
        action as value_field,
        count(*) as n_records

    from `dbt`.`int_celo_gpay_activity`
    group by action

)

select *
from all_values
where value_field not in (
    'Payment','Other','Withdrawal','Reversal','Top-up','Cashback'
)


