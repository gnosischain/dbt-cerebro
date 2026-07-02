
    
    

select
    quarter as unique_field,
    count(*) as n_records

from `dbt`.`api_quarterly_data_gnosis_app_peak_swappers`
where quarter is not null
group by quarter
having count(*) > 1


