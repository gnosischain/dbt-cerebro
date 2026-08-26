
    
    

with all_values as (

    select
        card_state as value_field,
        count(*) as n_records

    from `dbt`.`fct_celo_gpay_cardholder_engagement`
    group by card_state

)

select *
from all_values
where value_field not in (
    'never_funded','funded_never_paid','active','dormant_7_30','dormant_30plus'
)


