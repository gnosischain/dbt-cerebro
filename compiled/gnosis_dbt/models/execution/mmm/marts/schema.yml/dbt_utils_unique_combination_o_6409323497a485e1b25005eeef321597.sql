





with validation_errors as (

    select
        col_a, col_b
    from `dbt`.`fct_execution_mmm_collinearity_latest`
    group by col_a, col_b
    having count(*) > 1

)

select *
from validation_errors


