



select
    1
from `dbt`.`fct_celo_gpay_settlement_batches`

where not(settlement_label != 'unknown')

