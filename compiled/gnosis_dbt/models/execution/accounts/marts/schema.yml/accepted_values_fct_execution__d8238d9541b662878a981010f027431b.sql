
    
    

with all_values as (

    select
        result_type as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_account_search_index`
    group by result_type

)

select *
from all_values
where value_field not in (
    'address','safe','gpay_wallet','gnosis_app_user','circles','validator','validator_credential'
)


