
    
    

with all_values as (

    select
        liveness_source as value_field,
        count(*) as n_records

    from `dbt`.`int_hopr_nodes`
    group by liveness_source

)

select *
from all_values
where value_field not in (
    'prober','not_probed','no_prober_for_network'
)


