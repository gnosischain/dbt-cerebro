





with validation_errors as (

    select
        date, offer_address
    from `dbt`.`int_execution_gnosis_app_token_offer_claim_funnel_daily`
    group by date, offer_address
    having count(*) > 1

)

select *
from validation_errors


