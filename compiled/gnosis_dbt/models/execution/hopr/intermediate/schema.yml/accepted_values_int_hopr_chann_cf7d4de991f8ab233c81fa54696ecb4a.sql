
    
    

with all_values as (

    select
        packing_overflow_check as value_field,
        count(*) as n_records

    from (select * from `dbt`.`int_hopr_channels_events` where packing_overflow_check IS NOT NULL) dbt_subquery
    group by packing_overflow_check

)

select *
from all_values
where value_field not in (
    '0'
)


