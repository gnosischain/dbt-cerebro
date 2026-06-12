





with validation_errors as (

    select
        new_safe, symbol
    from `dbt`.`int_execution_gpay_refunds`
    group by new_safe, symbol
    having count(*) > 1

)

select *
from validation_errors


