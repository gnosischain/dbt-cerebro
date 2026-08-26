

-- How long GP card funding wallets had been active on Celo before they funded a card.
-- One row per tenure bucket.
--
-- READ 'pre_dates_our_window' AS "AT LEAST THIS LONG, UNKNOWN HOW MUCH LONGER". Those
-- wallets were already transacting when celo_execution's coverage opens at the L2
-- migration, so their true tenure is unmeasurable here and is certainly longer than any
-- number this model could print. They are the strongest evidence of cross-sell into an
-- existing base, not a gap in the data. See fct_celo_gpay_wallet_tenure.
SELECT
    tenure_bucket                          AS label,
    count()                                AS value,
    -- Exact for uncensored buckets; a lower bound inside 'pre_dates_our_window'.
    round(median(days_before_first_funding), 1) AS median_days_before_funding,
    (SELECT max(first_funded_at) FROM `dbt`.`fct_celo_gpay_wallet_tenure`) AS as_of_date
FROM `dbt`.`fct_celo_gpay_wallet_tenure`
GROUP BY tenure_bucket
ORDER BY value DESC