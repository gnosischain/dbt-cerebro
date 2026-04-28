
    
    

select
    date as unique_field,
    count(*) as n_records

from (select * from `dbt`.`fct_consensus_attestations_performance_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is not null
group by date
having count(*) > 1


