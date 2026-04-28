

WITH mp_users AS (
    SELECT DISTINCT user_id_hash
    FROM `dbt`.`stg_mixpanel_ga__events`
    WHERE is_production = 1
      AND is_identified = 1
),

matches AS (
    SELECT
        mp.user_id_hash,
        id.gp_safe,
        groupArray(DISTINCT id.identity_role) AS matched_roles
    FROM mp_users mp
    INNER JOIN `dbt`.`int_execution_gpay_safe_identities` id
        ON mp.user_id_hash = id.user_pseudonym
    GROUP BY mp.user_id_hash, id.gp_safe
),

modules_per_safe AS (
    SELECT gp_safe, groupArray(contract_type) AS enabled_modules
    FROM `dbt`.`int_execution_gpay_safe_modules`
    GROUP BY gp_safe
),

allowance_per_safe AS (
    SELECT
        gp_safe,
        any(refill) AS daily_limit,
        any(period) AS allowance_period_seconds
    FROM `dbt`.`int_execution_gpay_allowances_current`
    GROUP BY gp_safe
),

delay_30d AS (
    SELECT gp_safe, sum(tx_added_count) AS delay_txs_last_30d
    FROM `dbt`.`int_execution_gpay_delay_activity_daily`
    WHERE date >= today() - 30
    GROUP BY gp_safe
),

spends_30d AS (
    SELECT gp_safe, sum(spend_count) AS spends_last_30d
    FROM `dbt`.`int_execution_gpay_spend_activity_daily`
    WHERE date >= today() - 30
    GROUP BY gp_safe
)

-- Explicit AS aliases on the m.* columns are required. ClickHouse keeps
-- the qualified name `m.gp_safe` in the output projection whenever the
-- bare name `gp_safe` is ambiguous across joined relations (modules_per_safe,
-- allowance_per_safe, delay_30d, spends_30d all expose a `gp_safe` column),
-- and the config's order_by=(user_id_hash, gp_safe) then fails to resolve.
SELECT
    m.user_id_hash   AS user_id_hash,
    m.gp_safe        AS gp_safe,
    m.matched_roles  AS matched_roles,
    coalesce(mod.enabled_modules,        []::Array(String)) AS enabled_modules,
    al.daily_limit                                          AS daily_limit,
    al.allowance_period_seconds                             AS allowance_period_seconds,
    coalesce(da.delay_txs_last_30d, 0)                      AS delay_txs_last_30d,
    coalesce(sp.spends_last_30d, 0)                         AS spends_last_30d
FROM matches m
LEFT JOIN modules_per_safe   mod ON mod.gp_safe = m.gp_safe
LEFT JOIN allowance_per_safe al  ON al.gp_safe  = m.gp_safe
LEFT JOIN delay_30d          da  ON da.gp_safe  = m.gp_safe
LEFT JOIN spends_30d         sp  ON sp.gp_safe  = m.gp_safe