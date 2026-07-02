
    
    

select
    pay_wallet as unique_field,
    count(*) as n_records

from `dbt`.`int_execution_gnosis_app_gt_pay_wallets`
where pay_wallet is not null
group by pay_wallet
having count(*) > 1


