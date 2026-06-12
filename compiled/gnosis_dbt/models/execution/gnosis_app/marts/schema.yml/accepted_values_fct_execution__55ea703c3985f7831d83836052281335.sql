
    
    

with all_values as (

    select
        onboarding_class as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_gnosis_app_gpay_volume_daily`
    group by onboarding_class

)

select *
from all_values
where value_field not in (
    'onboarded_via_ga','imported'
)


