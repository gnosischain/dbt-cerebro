{{ config(severity='warn', tags=['production', 'data_quality', 'data_quality_daily', 'pools']) }}
-- Invariant: a DEX pool's reserve is its physical token balance, so it must (a) never be
-- negative for a solvent pool and (b) equal token_amount (both are now defined as the same
-- physical-balance delta: Burn/LiquidityRemoved-of-principal clamped, all else at face, swap
-- fees NOT subtracted). A negative reserve is the exact symptom of the retired fee-subtraction
-- drift -- the Swapr V3 sDAI/EURe pool 0x2de7439f went negative on 2026-06-26 after cumulative
-- swap fees (~7k sDAI over 2.5y) exceeded a thinned principal -- or of a cumulative-seed drift
-- / decode gap. A reserve != token_amount means the swap-fee subtraction (or a similar
-- divergence) has been reintroduced into one of the protocol daily models.
--
-- Stronger complementary check (periodic, NOT this test): reconcile sum(reserve_amount) per
-- (pool, token) to on-chain aToken/ERC20 balanceOf or a transfer-derived pool balance. A
-- pure non-negativity check alone can miss an OVER-count (reserve > true balance from a decode
-- duplicate); the reserve == token_amount clause here guards the definition, but only an
-- on-chain reconciliation bounds the absolute level.
SELECT
    date,
    protocol,
    pool_address,
    token_address,
    token,
    reserve_amount,
    token_amount,
    reserve_amount - token_amount AS reserve_minus_balance
FROM {{ ref('int_execution_pools_balances_daily') }}
WHERE date >= today() - {{ var('test_lookback_days', 7) }}
  -- only labeled tokens: unlabeled (token NULL) legs are Circles CRC20 wrappers whose reserves
  -- are owned/validated by the Circles pipeline (api_execution_circles_v2_pools_reserves_*),
  -- not by the general DEX-pools model.
  AND token IS NOT NULL
  AND (
        reserve_amount < -0.01
     OR abs(reserve_amount - token_amount) > greatest(0.01, 0.001 * abs(token_amount))
  )
ORDER BY reserve_amount
