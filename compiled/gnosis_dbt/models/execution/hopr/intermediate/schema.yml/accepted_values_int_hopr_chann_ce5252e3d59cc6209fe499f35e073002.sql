
    
    

with all_values as (

    select
        event_name as value_field,
        count(*) as n_records

    from `dbt`.`int_hopr_channels_events`
    group by event_name

)

select *
from all_values
where value_field not in (
    'ChannelOpened','ChannelBalanceIncreased','ChannelBalanceDecreased','TicketRedeemed','OutgoingChannelClosureInitiated','ChannelClosed'
)


