



select
    1
from `dbt`.`fct_celo_gpay_card_funnel`

where not(observation_days >= 0)

