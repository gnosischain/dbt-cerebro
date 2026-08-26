

-- Dense per-Safe/token/day net-flow balance. This is the Celo analog of
-- Gnosis Chain's int_execution_gpay_balances_user_daily, which is dense by
-- construction (it reads a chain-wide balance-diff snapshot). Celo has no
-- such snapshot source, so we DENSIFY here: a date spine x every
-- (safe, token) pair that has ever transacted, left-joined to daily net flow,
-- with the running total carried across the full range.
--
-- Why densify rather than reuse the sparse int_celo_gpay_balances_daily:
-- summing sparse per-Safe running totals by day undercounts, because on a
-- given day a Safe with no activity contributes no row (its carried-forward
-- balance would be missing from that day's SUM). Densifying first, then
-- aggregating downstream, makes both the daily balance time series AND the
-- latest-day snapshot correct (every Safe/token present on every day).
--
-- CORRECTNESS SCOPE: net flow == true on-chain balance holds for the whitelisted
-- tokens (celo_tokens_whitelist: USDT, USDC, USDm, XAUt0 — only USDT and USDC
-- carry any flow as of 2026-08-03), and only because Celo GP Safes start at zero
-- pre-launch and there are no Safe-to-Safe transfers (re-verified on the complete
-- backfill 2026-08-03: zero transfers with a registry Safe on both sides). Value
-- arriving in a NON-whitelisted token is outside this model by design — see
-- fct_celo_gpay_card_balances_alltoken_daily for the all-token view. Adding a real
-- token means adding it to celo_tokens_whitelist; that is now the only gate.
-- Inflows = Top-up + Reversal, outflows = Payment + Withdrawal.
--
-- VALUATION — balance_usd is MARK-TO-MARKET: the native running balance valued at
-- the token's price AS OF that date. It is deliberately NOT a running sum of each
-- flow's USD value; that would be an accumulated COST BASIS, which is a different
-- (and for this model wrong) quantity. Flows are valued at transaction time and
-- stocks as of the date they are reported — see int_celo_gpay_activity for the flow
-- side, whose amount_usd is correctly historical.
-- Why it matters: for USDT/USDC the two agree to ~0.02% (switching cost 4.29 USD on
-- a 25.6k float, 2026-08-04). For a volatile token they diverge without bound and
-- cost basis becomes nonsense: 1 XAUt0 in at 3,000 then 0.5 out at 4,042 leaves a
-- cost basis of 979 USD against a real holding of 2,021, and enough appreciation
-- drives it NEGATIVE while the card still holds gold. XAUt0 is whitelisted for the
-- announced cashback, so this had to be fixed before the first reward lands.
--
-- ASOF LEFT JOIN, not a join on date, because the price hub does NOT forward-fill
-- (int_celo_token_prices_daily emits only days with an observed answer). An
-- equi-join would read $0 on the first missed oracle day and silently collapse the
-- whole float. ASOF carries the last observed price forward, uncapped: a delisted
-- token would freeze at its final price, so staleness is asserted rather than capped
-- (price_date is exposed for exactly that, and the two warn-severity tests on this
-- model in schema.yml cover both the unpriced and the stale case). A cap would be
-- worse than the disease: it converts a stale price into a NULL, and a NULL is
-- skipped by sum() downstream, so the position would vanish from the total instead
-- of being visibly wrong.
-- A token with no price at or before the date yields NULL balance_usd, never 0 —
-- unknown must not read as empty. Coverage on 2026-08-04: 69/69 spine days priced
-- for every stablecoin, zero held days without an exact price.
--
-- token_class comes from celo_tokens_whitelist (STABLECOIN | RWA) and exists so
-- downstream headline figures can separate spendable float from reward holdings
-- without hardcoding a symbol list. See fct_celo_gpay_snapshots.
--
-- SCALING: this model cross-joins a date spine against every (safe, token) pair
-- and full-rebuilds, so it is O(cards x days) with no incremental path (62,997
-- rows at 913 pairs x 69 days on 2026-08-04). Revisit the materialisation before
-- the card base grows another order of magnitude.
WITH bounds AS (
    SELECT
        assumeNotNull(min(date)) AS min_date,
        assumeNotNull(max(date)) AS max_date
    FROM `dbt`.`int_celo_gpay_activity_daily`
),

date_spine AS (
    SELECT toDate((SELECT min_date FROM bounds) + number) AS date
    FROM numbers(assumeNotNull(toUInt64((SELECT max_date FROM bounds) - (SELECT min_date FROM bounds) + 1)))
),

pairs AS (
    SELECT DISTINCT safe_address, token_symbol
    FROM `dbt`.`int_celo_gpay_activity_daily`
),

daily_net AS (
    SELECT
        date,
        safe_address,
        token_symbol,
        SUM(CASE WHEN action IN ('Top-up', 'Reversal', 'Cashback') THEN amount     ELSE -amount     END) AS net_amount,
        SUM(CASE WHEN action IN ('Top-up', 'Reversal', 'Cashback') THEN amount_usd ELSE -amount_usd END) AS net_amount_usd
    FROM `dbt`.`int_celo_gpay_activity_daily`
    GROUP BY date, safe_address, token_symbol
),

grid AS (
    SELECT d.date, p.safe_address, p.token_symbol
    FROM date_spine d
    CROSS JOIN pairs p
),

running AS (
    SELECT
        g.date,
        g.safe_address,
        g.token_symbol,
        SUM(coalesce(n.net_amount, 0)) OVER (PARTITION BY g.safe_address, g.token_symbol ORDER BY g.date) AS balance
    FROM grid g
    LEFT JOIN daily_net n
        ON  n.date         = g.date
        AND n.safe_address = g.safe_address
        AND n.token_symbol = g.token_symbol
),

-- Last price at or before each date (see the VALUATION note in the header for why
-- ASOF rather than an equi-join on date).
prices AS (
    SELECT symbol, date, price
    FROM `dbt`.`int_celo_token_prices_daily`
    WHERE price > 0
),

valued AS (
    SELECT r.date, r.safe_address, r.token_symbol, r.balance, p.price, p.date AS price_date
    FROM running r
    ASOF LEFT JOIN prices p
        ON r.token_symbol = p.symbol
       AND p.date <= r.date
),

token_classes AS (
    SELECT
        lower(symbol)                  AS symbol_lower,
        argMax(token_class, date_start) AS token_class
    FROM `dbt`.`celo_tokens_whitelist`
    GROUP BY symbol_lower
)

SELECT
    v.date,
    v.safe_address,
    v.token_symbol,
    coalesce(nullIf(w.token_class, ''), 'UNKNOWN') AS token_class,
    v.balance,
    v.balance * v.price                            AS balance_usd,
    v.price_date                                   AS price_date
FROM valued v
LEFT JOIN token_classes w
    ON lower(v.token_symbol) = w.symbol_lower
SETTINGS join_use_nulls = 1