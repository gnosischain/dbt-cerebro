





with validation_errors as (

    select
        block_number, transaction_index, log_index
    from `dbt`.`contracts_BalancerV3_Vault_events_live`
    group by block_number, transaction_index, log_index
    having count(*) > 1

)

select *
from validation_errors


