
    
    

select
    avatar as unique_field,
    count(*) as n_records

from `dbt`.`int_execution_circles_v2_avatar_metadata`
where avatar is not null
group by avatar
having count(*) > 1


