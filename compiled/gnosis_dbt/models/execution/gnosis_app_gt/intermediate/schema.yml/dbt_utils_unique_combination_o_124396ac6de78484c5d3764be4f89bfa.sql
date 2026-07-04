





with validation_errors as (

    select
        card, ga_account, source
    from `dbt`.`int_execution_gnosis_app_gt_card_owner`
    group by card, ga_account, source
    having count(*) > 1

)

select *
from validation_errors


