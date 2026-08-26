



select
    1
from `dbt`.`int_celo_gpay_safe_transfers_alltoken`

where not(amount_raw <= toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'))

