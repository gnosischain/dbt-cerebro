





with validation_errors as (

    select
        quarter, peg_class, balance_bucket
    from `dbt`.`int_quarterly_stablecoin_cohorts_stats`
    group by quarter, peg_class, balance_bucket
    having count(*) > 1

)

select *
from validation_errors


