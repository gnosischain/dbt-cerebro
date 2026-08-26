

-- First-fund shape mix across funded cards. first_fund_channel is the channel of
-- each card's earliest inbound transfer (from int_celo_gpay_funding_tx_envelopes).
-- Unfunded cards (NULL channel) are excluded — this is a mix of how cards that
-- have been funded got their first money, not of the whole issued base.
--
-- as_of_date is the funnel's own horizon (observed_through), not today(): the mix is a
-- snapshot of cards observed up to that date. Taken as a scalar over the whole table
-- rather than max() per group — a per-group max would report each channel's last
-- funding date and read as if the channels were measured as of different days.
SELECT
    first_fund_channel AS label,
    count()            AS value,
    (SELECT max(observed_through) FROM `dbt`.`fct_celo_gpay_card_funnel`) AS as_of_date
FROM `dbt`.`fct_celo_gpay_card_funnel`
WHERE first_fund_channel IS NOT NULL
  AND first_fund_channel != ''
GROUP BY first_fund_channel
ORDER BY value DESC