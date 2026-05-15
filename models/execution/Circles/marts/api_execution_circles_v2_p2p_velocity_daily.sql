{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_p2p_velocity','granularity:daily']
  )
}}

-- "Circles velocity" series — daily peer-to-peer transfers only (excludes
-- mints, burns, wraps, unwraps). The plan calls for further subdivision
-- into p2p_direct / p2p_matrix once StreamCompleted lands.

SELECT
    date,
    sum(n_transfers) AS n_transfers,
    sum(n_senders)   AS n_senders,
    sum(n_receivers) AS n_receivers,
    sum(amount)      AS amount
FROM {{ ref('int_execution_circles_v2_transfers_daily') }}
WHERE transfer_category = 'p2p'
  AND date < today()
GROUP BY date
ORDER BY date DESC
