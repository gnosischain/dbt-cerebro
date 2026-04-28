
    
    

with all_values as (

    select
        relation as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_account_linked_entities_latest`
    group by relation

)

select *
from all_values
where value_field not in (
    'safe_owner_of','safe_owned_by','gnosis_app_controls_gpay_wallet','validator_withdrawal_credential'
)


