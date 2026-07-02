
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `dbt`.`stg_envio_ga__swaps`
    group by status

)

select *
from all_values
where value_field not in (
    'Filled','Expired','NONE','Open','PayTopUp'
)


