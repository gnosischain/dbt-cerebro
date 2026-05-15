

-- Pseudonymization boundary for the GP Safe identity graph. Projects the
-- internal-only bridge (which holds raw owner/delegate EOAs alongside the
-- Safe address) down to pseudonym + role + Safe so marts and downstream
-- consumers can read this safely. The bridge is the single source of
-- truth for the UNION of initial_owners ∪ delegates ∪ safe_self and for
-- the pseudonymize_address calls; this projection drops the raw address
-- column and the seen-at timestamps so the hash function is computed
-- exactly once across the lineage.

SELECT
    gp_safe,
    identity_role,
    user_pseudonym
FROM `dbt`.`int_execution_gpay_user_identity_bridge`