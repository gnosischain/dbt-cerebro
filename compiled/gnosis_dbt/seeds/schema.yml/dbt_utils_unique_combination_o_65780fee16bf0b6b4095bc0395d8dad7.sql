





with validation_errors as (

    select
        chain, contract_address, implementation_address
    from `dbt`.`contracts_abi`
    group by chain, contract_address, implementation_address
    having count(*) > 1

)

select *
from validation_errors


