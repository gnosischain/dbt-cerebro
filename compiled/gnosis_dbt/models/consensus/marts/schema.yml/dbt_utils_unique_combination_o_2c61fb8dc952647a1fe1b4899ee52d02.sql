





with validation_errors as (

    select
        label, graffiti
    from `dbt`.`fct_consensus_graffiti_cloud`
    group by label, graffiti
    having count(*) > 1

)

select *
from validation_errors


