



select
    1
from `dbt`.`int_execution_gnosis_app_gt_pay_wallets`

where not(n_active_modules >= 1)

