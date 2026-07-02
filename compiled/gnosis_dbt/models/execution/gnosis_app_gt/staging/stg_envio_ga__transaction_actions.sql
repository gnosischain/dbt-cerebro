

-- STRETCH: exactly ONE full scan of envio_ga.transaction_action (208M rows;
-- id-sorted, so `timestamp`/`_synced_block` do NOT prune — EXPLAIN ESTIMATE reads
-- all 208M). Reduces the per-action feed to per-identity recency: first/last
-- action time + total action count. This is the missing on-chain timestamp for
-- the ground-truth suite (envio_ga entity tables have none).
--
-- avatar_id covers the WHOLE Circles ecosystem back to 2020 (~626k avatars), so
-- this table alone is NOT a user metric — int_execution_gnosis_app_gt_user_activity
-- joins it to registry/avatar identities only.
SELECT
    lower(avatar_id)            AS address,
    toDateTime(min(timestamp))  AS first_action_at,
    toDateTime(max(timestamp))  AS last_action_at,
    count()                     AS n_actions
FROM `envio_ga`.`transaction_action`
WHERE _deleted = 0
  AND avatar_id != ''
GROUP BY lower(avatar_id)