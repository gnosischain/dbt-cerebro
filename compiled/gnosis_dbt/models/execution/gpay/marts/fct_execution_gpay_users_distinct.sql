

-- Deduped user-pseudonym projection for the semantic-layer's cross-sector
-- user-overlap path. The source int_execution_gpay_safe_identities has
-- 1 row per (user_pseudonym, identity_role, gp_safe) — multiple rows per
-- user. This view collapses to 1 row per user_pseudonym, with
-- sector-presence flags so analysts can filter by role without re-joining.
--
-- Why a separate view: the cerebro-dev semantic-layer planner emits the
-- `count_distinct` agg as a raw SQL function name, which ClickHouse
-- doesn't expose (uniqExact is the ClickHouse equivalent). To stay
-- compatible, every user-keyed semantic_model points at a mart that's
-- already at 1-row-per-user grain so the metric can use `agg: count`.

SELECT
    user_pseudonym,
    max(identity_role = 'initial_owner')  AS has_initial_owner,
    max(identity_role = 'delegate')       AS has_delegate,
    max(identity_role = 'safe_self')      AS has_safe_self,
    uniqExact(gp_safe)                    AS n_safes,
    uniqExact(identity_role)              AS n_roles
FROM `dbt`.`int_execution_gpay_safe_identities`
WHERE user_pseudonym IS NOT NULL
GROUP BY user_pseudonym