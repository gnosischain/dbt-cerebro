
    
    

with all_values as (

    select
        contract_type as value_field,
        count(*) as n_records

    from `dbt`.`contracts_hopr_registry`
    group by contract_type

)

select *
from all_values
where value_field not in (
    'Channels','Announcements','NodeStakeFactory','NodeSafeRegistry','NodeSafeMigration','TicketPriceOracle','WinningProbabilityOracle','NetworkRegistry'
)


