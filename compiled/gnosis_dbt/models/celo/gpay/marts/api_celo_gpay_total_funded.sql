

-- All-time distinct cards that have ever RECEIVED money. RESTATED 2026-08-05: this
-- read the PaymentUsers snapshot until then, i.e. cards that had ever SPENT, so the
-- funded tile showed 662 against a true 1087. The old value is exposed as
-- api_celo_gpay_total_activated rather than being dropped.
SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_celo_gpay_activity_daily`) AS as_of_date
FROM (
SELECT value
FROM `dbt`.`fct_celo_gpay_snapshots`
WHERE label = 'FundedCards' AND window = 'All'
) AS sub