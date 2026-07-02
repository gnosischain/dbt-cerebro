





with validation_errors as (

    select
        quarter, peg_class, balance_bucket
    from `dbt`.`api_quarterly_data_stablecoin_holder_cohorts`
    group by quarter, peg_class, balance_bucket
    having count(*) > 1

)

select *
from validation_errors


