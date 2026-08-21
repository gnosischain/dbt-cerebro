
    
    

with all_values as (

    select
        node_class as value_field,
        count(*) as n_records

    from `dbt`.`int_hopr_nodes`
    group by node_class

)

select *
from all_values
where value_field not in (
    'cover_traffic','gnosisvpn_exit','unclassified'
)


