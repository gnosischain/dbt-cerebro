





with validation_errors as (

    select
        app_scope, status
    from `dbt`.`fct_execution_gnosis_app_gt_swaps_summary`
    group by app_scope, status
    having count(*) > 1

)

select *
from validation_errors


