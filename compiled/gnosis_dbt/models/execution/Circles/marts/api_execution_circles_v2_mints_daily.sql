

SELECT
    date,
    n_mint_events,
    n_minters,
    amount_minted
FROM `dbt`.`int_execution_circles_v2_mints_daily`
WHERE date < today()
ORDER BY date DESC