{{ config(materialized='table') }}

SELECT
  addDays(toDate('2018-10-08'), number) AS day
FROM numbers(
  dateDiff('day', toDate('2018-10-08'), addYears(today(), 5)) + 1
)
