





with validation_errors as (

    select
        quarter, peg_class, tokens_included
    from `dbt`.`api_quarterly_data_stablecoin_transfers`
    group by quarter, peg_class, tokens_included
    having count(*) > 1

)

select *
from validation_errors


