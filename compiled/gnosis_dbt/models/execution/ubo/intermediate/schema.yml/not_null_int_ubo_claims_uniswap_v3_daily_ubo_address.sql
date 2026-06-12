
    
    



select ubo_address
from (select * from `dbt`.`int_ubo_claims_uniswap_v3_daily` where toDate(date) >= today() - 7) dbt_subquery
where ubo_address is null


