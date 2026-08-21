{{
    config(
        materialized='view',
        tags=['production','hopr','tier1','api:hopr_protocol_params','granularity:daily']
    )
}}

/*
  Daily protocol parameters and staked/locked wxHOPR from blokli, HOPR's own v4 indexer.

  JURA ONLY BY DESIGN. blokli serves jura and rotsee; rotsee is a testnet whose ticket
  price and balances sit orders of magnitude away from production, so it is filtered out
  here rather than published with a flag. Nothing in this view covers dufour -- blokli has
  no dufour endpoint, and dufour economics come from the decoded channel events instead
  (api_hopr_channel_activity_daily).

  payout_per_winning_ticket_wxhopr is the number worth showing. HOPR pays relayers
  probabilistically: a ticket wins with probability p and pays ticket_price when it does,
  so price alone says nothing and is not comparable across networks running different
  probabilities. The division is done upstream in the staging view.

  FORWARD-ONLY. blokli is queried for today, so history begins when the click-runner
  ingestor first ran and no earlier data can ever be recovered. Expect a short series and
  a gap for any day the job did not run.
*/

SELECT
    network,
    snapshot_date                                                   AS date,
    ticket_price_wxhopr,
    min_ticket_winning_probability,
    payout_per_winning_ticket_wxhopr,
    key_binding_fee_wxhopr,
    channel_closure_grace_period_s,
    account_count,
    safes_count,
    safes_balance_wxhopr,
    channels_total,
    channels_open,
    channels_open_balance_wxhopr,
    channels_pendingtoclose,
    channels_pendingtoclose_balance_wxhopr,
    -- Total wxHOPR the network holds: sitting in Safes plus locked in live channels. The
    -- closest thing to a TVL for HOPR, and it needs no balance indexing.
    safes_balance_wxhopr + channels_open_balance_wxhopr             AS total_wxhopr_committed
FROM {{ ref('stg_hopr_db__blokli_network_snapshot') }}
WHERE NOT is_testnet
ORDER BY network, date
