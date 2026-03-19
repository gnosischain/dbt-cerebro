

SELECT
    d.date,
    arrayJoin(bitmapToArray(d.ua_bitmap_state)) AS address_hash
FROM `dbt`.`int_execution_transactions_by_project_daily` d
WHERE d.date > subtractDays(today(), 181)