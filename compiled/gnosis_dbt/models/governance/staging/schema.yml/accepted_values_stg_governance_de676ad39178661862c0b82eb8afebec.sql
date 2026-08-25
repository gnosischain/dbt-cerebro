
    
    

with all_values as (

    select
        option_polarity as value_field,
        count(*) as n_records

    from `dbt`.`stg_governance__forum_polls`
    group by option_polarity

)

select *
from all_values
where value_field not in (
    'for','against','abstain','other'
)


