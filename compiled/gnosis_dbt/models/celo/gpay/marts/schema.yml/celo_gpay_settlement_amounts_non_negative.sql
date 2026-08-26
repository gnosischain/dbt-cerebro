



select
    1
from `dbt`.`fct_celo_gpay_settlement_batches`

where not(charged_amount >= 0 AND settled_amount >= 0 AND native_fee_celo >= 0)

