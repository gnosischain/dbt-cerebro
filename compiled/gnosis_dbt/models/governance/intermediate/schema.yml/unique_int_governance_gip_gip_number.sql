
    
    

select
    gip_number as unique_field,
    count(*) as n_records

from `dbt`.`int_governance_gip`
where gip_number is not null
group by gip_number
having count(*) > 1


