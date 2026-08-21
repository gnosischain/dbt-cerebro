



select
    1
from (select * from `dbt`.`contracts_hopr_TicketPriceOracle_events` where event_name = 'TicketPriceUpdated') dbt_subquery

where not(has(mapKeys(decoded_params), 'newTicketPrice'))

