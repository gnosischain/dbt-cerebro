
    
    

with all_values as (

    select
        network as value_field,
        count(*) as n_records

    from `dbt`.`fct_hopr_gnosisvpn_users_daily`
    group by network

)

select *
from all_values
where value_field not in (
    'dufour','jura'
)


