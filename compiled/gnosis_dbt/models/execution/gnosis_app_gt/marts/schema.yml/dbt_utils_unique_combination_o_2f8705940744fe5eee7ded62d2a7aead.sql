





with validation_errors as (

    select
        app_scope, sell_token, buy_token
    from `dbt`.`fct_execution_gnosis_app_gt_swaps_by_pair`
    group by app_scope, sell_token, buy_token
    having count(*) > 1

)

select *
from validation_errors


