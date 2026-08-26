



select
    1
from (select * from `dbt`.`fct_celo_gpay_card_funnel` where is_activated) dbt_subquery

where not(is_funded)

