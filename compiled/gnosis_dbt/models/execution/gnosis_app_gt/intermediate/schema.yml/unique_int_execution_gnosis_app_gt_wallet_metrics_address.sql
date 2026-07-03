
    
    

select
    address as unique_field,
    count(*) as n_records

from `dbt`.`int_execution_gnosis_app_gt_wallet_metrics`
where address is not null
group by address
having count(*) > 1


