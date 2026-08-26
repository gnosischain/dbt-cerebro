
    
    

select
    cohort_month as unique_field,
    count(*) as n_records

from `dbt`.`fct_celo_gpay_funnel_cohorts_monthly`
where cohort_month is not null
group by cohort_month
having count(*) > 1


