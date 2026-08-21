





with validation_errors as (

    select
        chain, contract_address, implementation_address, signature
    from `dbt`.`function_signatures`
    group by chain, contract_address, implementation_address, signature
    having count(*) > 1

)

select *
from validation_errors


