

-- Swap counts by app_scope x real CoW status enum. app_scope separates the apps
-- that share the Circles indexer: gnosis_app (the actual Gnosis App), metri (a
-- DIFFERENT app), third_party (other CoW UIs), unknown (empty metadata), test.
-- For Gnosis-App-only reporting, filter app_scope='gnosis_app' (do NOT sum
-- gnosis_app+metri — that conflates two apps).
SELECT
    app_scope,
    status,
    count()             AS n_swaps,
    uniqExact(owner)    AS n_swappers
FROM `dbt`.`stg_envio_ga__swaps`
GROUP BY app_scope, status