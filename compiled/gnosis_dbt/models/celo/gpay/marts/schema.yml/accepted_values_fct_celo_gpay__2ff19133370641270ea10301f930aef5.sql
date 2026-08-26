
    
    

with all_values as (

    select
        tenure_bucket as value_field,
        count(*) as n_records

    from `dbt`.`fct_celo_gpay_wallet_tenure`
    group by tenure_bucket

)

select *
from all_values
where value_field not in (
    'unknown','pre_dates_our_window','same_day','1_7_days','8_30_days','31_90_days','91_180_days','over_180_days'
)


