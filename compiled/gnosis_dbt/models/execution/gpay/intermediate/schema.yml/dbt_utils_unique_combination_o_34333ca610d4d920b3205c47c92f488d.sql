





with validation_errors as (

    select
        pay_wallet, owner
    from (select * from `dbt`.`int_execution_gpay_wallet_owners` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by pay_wallet, owner
    having count(*) > 1

)

select *
from validation_errors


