
    
    



select date
from (select * from `dbt`.`int_GBCDeposit_deposists_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


