
    
    



select Date
from (select * from `dbt`.`stg_crawlers_data__ember_electricity_data` where toDate(Date) >= today() - 7) dbt_subquery
where Date is null


