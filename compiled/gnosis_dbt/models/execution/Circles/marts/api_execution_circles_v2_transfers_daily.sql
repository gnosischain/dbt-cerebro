

SELECT
    date,
    transfer_category,
    n_transfers,
    n_senders,
    n_receivers,
    amount,
    amount_demurraged
FROM `dbt`.`int_execution_circles_v2_transfers_daily`
WHERE date < today()
ORDER BY date DESC, transfer_category