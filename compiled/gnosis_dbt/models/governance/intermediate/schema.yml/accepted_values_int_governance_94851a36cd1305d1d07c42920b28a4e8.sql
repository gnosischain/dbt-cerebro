
    
    

with all_values as (

    select
        ballot_type as value_field,
        count(*) as n_records

    from `dbt`.`int_governance_vote_choices`
    group by ballot_type

)

select *
from all_values
where value_field not in (
    'basic','single-choice','ranked-choice'
)


