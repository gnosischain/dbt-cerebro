





with validation_errors as (

    select
        root_address, relation, entity_id
    from `dbt`.`fct_execution_account_linked_entities_latest`
    group by root_address, relation, entity_id
    having count(*) > 1

)

select *
from validation_errors


