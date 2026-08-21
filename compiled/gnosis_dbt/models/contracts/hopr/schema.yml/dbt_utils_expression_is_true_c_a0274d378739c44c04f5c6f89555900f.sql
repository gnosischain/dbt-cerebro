



select
    1
from (select * from `dbt`.`contracts_hopr_WinningProbabilityOracle_events` where event_name = 'WinProbUpdated') dbt_subquery

where not(has(mapKeys(decoded_params), 'newWinProb'))

