





with validation_errors as (

    select
        mint_date, status
    from `dbt`.`fct_execution_gnosis_app_gt_cashback_nft`
    group by mint_date, status
    having count(*) > 1

)

select *
from validation_errors


