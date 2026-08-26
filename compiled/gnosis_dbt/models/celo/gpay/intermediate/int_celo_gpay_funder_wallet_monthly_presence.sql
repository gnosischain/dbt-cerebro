







-- One row per (month, funding wallet): the earliest and latest Transfer leg that wallet
-- had in that month, plus leg counts. The batching-safe substrate for wallet TENURE.
--
-- WHY A MONTHLY PRESENCE TABLE RATHER THAN A DIRECT min() PER WALLET. Tenure is
-- fundamentally `min(block_timestamp) per wallet` over the entire chain history, and the
-- natural shape for that is a one-row-per-wallet table. But a `table` model CANNOT be
-- batched: scripts/full_refresh/refresh.py re-runs the model per batch, and a table
-- materialisation does CREATE-OR-REPLACE every time, so each month would overwrite the
-- last and the "minimum" would end up being the minimum of the FINAL batch only —
-- silently, with a plausible-looking answer. That is an established rule in this repo
-- (never put meta.full_refresh on a table model). Folding presence per month into an
-- incremental, partition-per-month table sidesteps it entirely: each batch writes its
-- own partition, nothing overwrites anything, and the min() is taken downstream over a
-- table that is ~1k wallets x ~18 months instead of over 2.7 billion raw logs.
--
-- SCOPE IS DELIBERATELY WIDER THAN THE REST OF THE WALLET TREE, IN TWO WAYS.
--   1. The floor is the start of coverage (2025-03), not the GP era (2026-01), because
--      the entire question is what these wallets were doing BEFORE Gnosis Pay existed.
--   2. Fee legs are COUNTED here, unlike int_celo_gpay_funder_wallet_daily. A CIP-64 gas
--      receipt is not economic activity, but it is unambiguous proof the wallet existed
--      and transacted that month, which is exactly what tenure asks. n_legs_nonfee is
--      carried alongside so the stricter reading stays available.
--
-- Left-censoring is handled downstream in int_celo_gpay_funder_wallet_tenure, not here.
-- This model reports what it observed; it does not claim the first month it saw a wallet
-- is the month that wallet was born.
--
-- COST: the heaviest model in this tree — an all-Transfer scan across ~18 months rather
-- than the ~8 the GP-era models cover. Same hazard as its siblings: a single-query full
-- refresh OOMs (ClickHouse code 241). Rebuild ONLY through
-- scripts/full_refresh/refresh.py in the monthly batches declared in meta.full_refresh.
-- Output is tiny (one row per wallet-month), so the expense is entirely in the scan.

WITH wallets AS (
    SELECT lower(replaceAll(wallet_address, '0x', '')) AS addr
    FROM `dbt`.`int_celo_gpay_funder_wallets`
),

transfer_logs AS (
    SELECT * FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY block_number, transaction_index, log_index
                ORDER BY insert_version DESC
            ) AS _dedup_rn
        FROM `celo_execution`.`logs`
        WHERE replaceAll(topic0, '0x', '') = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- Transfer
          AND block_timestamp >= toDateTime('2025-03-01')
          
          
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.month)), -0))
        FROM `dbt`.`int_celo_gpay_funder_wallet_monthly_presence` AS x1
        WHERE 1=1 
      )
      
    
  

    )
    WHERE _dedup_rn = 1
),

-- Both sides of the transfer count as presence: being paid is as much evidence the
-- wallet existed as paying. arrayJoin over the matching sides gives one row per
-- (leg, wallet-side), matching the grain convention of the GP-era transfer model.
legs AS (
    SELECT
        l.block_timestamp                                            AS block_time,
        arrayJoin(arrayFilter(
            x -> x IN (SELECT addr FROM wallets),
            [substring(replaceAll(l.topic1, '0x', ''), 25, 40),
             substring(replaceAll(l.topic2, '0x', ''), 25, 40)]
        ))                                                           AS wallet_raw,
        substring(replaceAll(l.topic2, '0x', ''), 25, 40)            AS to_raw
    FROM transfer_logs l
    WHERE substring(replaceAll(l.topic1, '0x', ''), 25, 40) IN (SELECT addr FROM wallets)
       OR substring(replaceAll(l.topic2, '0x', ''), 25, 40) IN (SELECT addr FROM wallets)
)

SELECT
    toStartOfMonth(toDate(block_time))                               AS month,
    concat('0x', wallet_raw)                                         AS wallet_address,
    min(block_time)                                                  AS first_seen_at,
    max(block_time)                                                  AS last_seen_at,
    count()                                                          AS n_legs,
    -- The CIP-64 gas sink, excluded here only so the stricter "did something economic"
    -- reading stays available downstream. Presence itself counts every leg.
    countIf(to_raw != '000000000000000000000000000000000ce106a5')    AS n_legs_nonfee
FROM legs
GROUP BY month, wallet_address