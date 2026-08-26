



select
    1
from (select * from `dbt`.`fct_celo_gpay_settlement_batches` where n_bridged = 0) dbt_subquery

where not(native_fee_celo = 0)

