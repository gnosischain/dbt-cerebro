
    
    



select week
from (select * from `dbt`.`api_execution_gpay_activity_by_action_weekly` where toDate(week) >= today() - 7) dbt_subquery
where week is null


