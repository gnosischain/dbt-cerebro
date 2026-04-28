
    
    

with all_values as (

    select
        edge_type as value_field,
        count(*) as n_records

    from `dbt`.`fct_execution_account_counterparty_edges_latest`
    group by edge_type

)

select *
from all_values
where value_field not in (
    'token_transfer','gpay_activity','safe_relation','circles_trust','validator_relation'
)


