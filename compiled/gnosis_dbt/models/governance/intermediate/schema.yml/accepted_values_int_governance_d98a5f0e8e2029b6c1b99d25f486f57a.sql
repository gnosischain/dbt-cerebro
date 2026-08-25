
    
    

with all_values as (

    select
        polarity as value_field,
        count(*) as n_records

    from `dbt`.`int_governance_vote_choices`
    group by polarity

)

select *
from all_values
where value_field not in (
    'for','against','abstain','other','unknown'
)


