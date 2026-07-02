





with validation_errors as (

    select
        quarter, peg_class
    from `dbt`.`api_quarterly_data_stablecoin_supply`
    group by quarter, peg_class
    having count(*) > 1

)

select *
from validation_errors


