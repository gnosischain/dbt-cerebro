



select
    1
from `dbt`.`fct_execution_account_profile_latest`

where not(wallet_age_date wallet_age_date IS NULL OR (wallet_age_date > toDate('1970-01-01') AND wallet_age_date <= today()))

