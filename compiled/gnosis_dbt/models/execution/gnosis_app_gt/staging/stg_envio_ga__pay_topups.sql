

-- STRETCH: one full scan of envio_ga.transfer, reduced to distinct (card, funder) for
-- app-initiated top-ups (PayTopUp / AutoTopup). funder (`from`) is the GA app account that
-- funded the card (`to`) through the app — a card -> GA-account link consumed by
-- int_execution_gnosis_app_gt_card_owner (gated there to registered GA accounts).
SELECT DISTINCT
    lower("to")   AS card,
    lower("from") AS funder
FROM `envio_ga`.`transfer`
WHERE _deleted = 0
  AND transfer_type IN ('PayTopUp', 'AutoTopup')
  AND "to"   != ''
  AND "from" != ''