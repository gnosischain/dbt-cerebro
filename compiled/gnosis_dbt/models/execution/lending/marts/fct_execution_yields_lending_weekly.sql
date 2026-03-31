

WITH weekly_agg AS (
    SELECT
        toStartOfWeek(date) AS week,
        token_address,
        symbol,
        token_class,
        protocol,
        -- APY: use last value of the week
        argMax(apy_daily, date) AS apy_weekly,
        argMax(borrow_apy_variable_daily, date) AS borrow_apy_weekly,
        groupBitmapMerge(lenders_bitmap_state) AS lenders_bitmap_state,
        groupBitmapMerge(borrowers_bitmap_state) AS borrowers_bitmap_state,
        sum(deposits_volume_daily) AS deposits_volume_weekly,
        sum(borrows_volume_daily) AS borrows_volume_weekly
    FROM `dbt`.`int_execution_yields_aave_daily`
    WHERE date < toStartOfWeek(today())
    GROUP BY week, token_address, symbol, token_class, protocol
)

SELECT
    week,
    token_address,
    symbol,
    token_class,
    protocol,
    apy_weekly,
    borrow_apy_weekly,
    lenders_bitmap_state,
    borrowers_bitmap_state,
    toUInt64(lenders_bitmap_state) AS lenders_count_weekly,
    toUInt64(borrowers_bitmap_state) AS borrowers_count_weekly,
    deposits_volume_weekly,
    borrows_volume_weekly
FROM weekly_agg