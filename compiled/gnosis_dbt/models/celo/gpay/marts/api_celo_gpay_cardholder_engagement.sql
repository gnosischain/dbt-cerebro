

-- Current card population split by engagement segment: for cards that are not being
-- used, is the holder still active on Celo at all? See fct_celo_gpay_cardholder_engagement
-- for the horizon-clipping and for why '__wallet_unmapped' is a coverage limit rather
-- than an observation of absence.
-- wallet_out_usd_retail is the column to size a reactivation opportunity on. The
-- unsuffixed total is published beside it because they diverge by an order of
-- magnitude — a handful of treasury-scale transfers dominate the raw sum — and a
-- reader who sees only one of them will quote the wrong one.
SELECT
    engagement_segment                            AS label,
    count()                                       AS value,
    round(sum(wallet_out_usd_recent_retail), 2)   AS wallet_out_usd_retail,
    round(sum(wallet_out_usd_recent), 2)          AS wallet_out_usd_total,
    max(observed_through)                         AS as_of_date
FROM `dbt`.`fct_celo_gpay_cardholder_engagement`
GROUP BY engagement_segment
ORDER BY value DESC