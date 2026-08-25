
    
    

with all_values as (

    select
        choice_kind as value_field,
        count(*) as n_records

    from `dbt`.`int_governance_vote_choices`
    group by choice_kind

)

select *
from all_values
where value_field not in (
    'single','ranked'
)


