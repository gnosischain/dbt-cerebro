





with validation_errors as (

    select
        order_uid, tx_hash, log_index
    from `dbt`.`stg_crawlers_data__cow_api_trade_fees`
    group by order_uid, tx_hash, log_index
    having count(*) > 1

)

select *
from validation_errors


