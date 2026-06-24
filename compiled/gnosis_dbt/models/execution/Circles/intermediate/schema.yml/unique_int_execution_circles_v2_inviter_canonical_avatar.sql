
    
    

select
    avatar as unique_field,
    count(*) as n_records

from `dbt`.`int_execution_circles_v2_inviter_canonical`
where avatar is not null
group by avatar
having count(*) > 1


