





with validation_errors as (

    select
        period_type, period_start
    from `dbt`.`fct_execution_gnosis_app_gt_active_wallets`
    group by period_type, period_start
    having count(*) > 1

)

select *
from validation_errors


