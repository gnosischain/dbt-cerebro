
    
    



select date
from (select * from `dbt`.`api_esg_cif_network_vs_countries_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


