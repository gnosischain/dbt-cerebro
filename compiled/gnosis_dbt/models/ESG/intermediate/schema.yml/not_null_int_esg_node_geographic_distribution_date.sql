
    
    



select date
from (select * from `dbt`.`int_esg_node_geographic_distribution` where toDate(date) >= today() - 7) dbt_subquery
where date is null


