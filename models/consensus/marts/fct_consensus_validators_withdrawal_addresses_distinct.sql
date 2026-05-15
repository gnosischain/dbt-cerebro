{{
  config(
    materialized='view',
    tags=['production','consensus','validators','mixpanel']
  )
}}

-- Per-pseudonym projection of validator withdrawal-address controllers.
-- One row per `user_pseudonym` (sipHash of the EVM withdrawal address).
-- Aggregates the number of validators each address controls so analysts
-- can both COUNT addresses (cardinality) and SUM stake concentration
-- (validators_controlled).
--
-- ## Cross-sector role
-- Closes the user_pseudonym graph on the consensus side. The 0x01-type
-- withdrawal credentials are EVM addresses, hashed via the project-wide
-- `pseudonymize_address` macro — same hash space as revenue / gpay /
-- gnosis_app / Circles. Enables queries like:
--   - "validators (by withdrawal address) who are also gpay users"
--   - "revenue-active users who are validator operators"
--   - "Circles humans staking on Gnosis (the populist-validator cohort)"
--
-- ## What's NOT here
-- - 0x00-type BLS-credential validators. These don't expose an EVM
--   withdrawal address (the credential is a hash of a BLS pubkey, not an
--   EVM address), so they can't be hashed into the user_pseudonym space.
--   ~5k validators fall in this bucket as of writing — captured in
--   total_0x00_validators_excluded so analysts can size the gap.
-- - Pool / staking-service attribution. Many of these addresses are
--   operated by Stakewise, Lido (Gnosis variant), etc. — labels live in
--   int_consensus_validators_labels but we don't denormalise them here
--   (the entity-graph relationship in execution_graph.yml is the right
--   surface for that).

WITH per_address AS (
    SELECT
        withdrawal_address,
        count() AS n_validators_controlled
    FROM {{ ref('int_consensus_validators_withdrawal_addresses') }}
    WHERE withdrawal_address IS NOT NULL
    GROUP BY withdrawal_address
)

SELECT
    {{ pseudonymize_address('withdrawal_address') }}  AS user_pseudonym,
    n_validators_controlled,
    -- Concentration tiers — useful as a categorical dimension. The cutoffs
    -- mirror the Gnosis Beacon-Chain validator-operator concentration
    -- buckets used in network-health reports.
    multiIf(
        n_validators_controlled = 1,        'single',
        n_validators_controlled <= 10,      'small (2-10)',
        n_validators_controlled <= 100,     'medium (11-100)',
        n_validators_controlled <= 1000,    'large (101-1000)',
                                            'whale (>1000)'
    )                                                  AS concentration_tier
FROM per_address
