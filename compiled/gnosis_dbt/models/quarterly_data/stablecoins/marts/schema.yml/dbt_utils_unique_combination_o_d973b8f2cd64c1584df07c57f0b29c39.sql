





with validation_errors as (

    select
        quarter, peg_class
    from `dbt`.`api_quarterly_data_stablecoin_holders`
    group by quarter, peg_class
    having count(*) > 1

)

select *
from validation_errors


