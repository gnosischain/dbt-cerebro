{{ config(severity='warn', tags=['production', 'data_quality', 'data_quality_weekly']) }}
-- Per contract, per month: log count in execution.logs (filtered to the contract) vs
-- row count in its decode model. A nonzero deficit = logs dropped below the append
-- watermark (or a raw hole) — the earliest, cheapest signal for the decode-layer drop
-- bug, firing long before cumulative balances go negative.
-- Lesson: decode-watermark-late-logs. Remediation: gap_window_refresh.py for exactly
-- the flagged months.
--
-- Extend with one UNION ALL arm per high-value contract (address is in each decode
-- model's decode_logs() call). Trailing 13 months keeps the scan bounded.
WITH raw_counts AS (
    SELECT
        'contracts_wxdai_events' AS decode_model,
        toStartOfMonth(block_timestamp) AS month,
        count() AS raw_logs
    FROM {{ source('execution', 'logs') }}
    WHERE address = 'e91d153e0b41518a2ce8dd3d7944fa863463a97d'  -- WxDAI, bare hex (no 0x)
      AND block_timestamp >= addMonths(toStartOfMonth(today()), -13)
    GROUP BY month
),
decoded_counts AS (
    SELECT
        'contracts_wxdai_events' AS decode_model,
        toStartOfMonth(block_timestamp) AS month,
        count() AS decoded_logs
    FROM {{ ref('contracts_wxdai_events') }}
    WHERE block_timestamp >= addMonths(toStartOfMonth(today()), -13)
    GROUP BY month
)
SELECT
    r.decode_model,
    r.month,
    r.raw_logs,
    d.decoded_logs,
    r.raw_logs - d.decoded_logs AS deficit
FROM raw_counts r
LEFT JOIN decoded_counts d ON d.decode_model = r.decode_model AND d.month = r.month
WHERE r.raw_logs - d.decoded_logs != 0
  -- exclude the open month: the decode watermark legitimately trails ingestion intraday
  AND r.month < toStartOfMonth(today())
ORDER BY r.month
