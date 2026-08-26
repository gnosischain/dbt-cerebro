



select
    1
from (select * from `dbt`.`fct_celo_gpay_card_funnel` where days_issue_to_fund IS NOT NULL) dbt_subquery

where not(days_issue_to_fund >= 0)

