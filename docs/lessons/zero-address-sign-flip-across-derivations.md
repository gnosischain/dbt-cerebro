---
id: zero-address-sign-flip-across-derivations
title: The zero address carries opposite signs in transfer-derived and state-derived balances
status: remediated
scope: >-
  any consumer that switches from a transfer-accumulated balance table to a state-census one
  (canonical: the 23 consumers of int_execution_tokens_balances_daily repointed to
  int_rpc_state_indexer_token_balances_priced_daily, WL-054 Stage 2)
symptom: >-
  after the switch a sum over all addresses jumps from about zero to circulating supply,
  a holder count gains one holder per token, or a per-address API starts publishing a
  0x000…000 "holder" — none of which any consumer filtered for, because their
  `balance > 0` had been doing it silently
last_verified: 2026-09-16
evidence:
  - 'GNO 2026-08-31 — transfer-derived zero-address balance -1,438,522 (mints counted as transfers out of 0x0), census zero-address 0; the two chains agree to 0.038% once 0x0 is excluded'
  - 'consumer inventory 2026-09-16 — six of 23 consumers summed balances without an explicit zero-address exclusion (int_ubo_claims_sdai_daily''s total_sdai_supply denominator the most exposed; fct_execution_tokens_overview_by_class_latest''s COUNT(DISTINCT address) WHERE balance_raw > 0 the next)'
  - 'decision (owner, 2026-09-16) — drop the zero address at the source in int_rpc_state_indexer_token_balances_daily (`holder_address != 0x0`) so every consumer is safe by construction; the burned balance is published once, in int_rpc_state_indexer_token_supply_daily.supply_total'
---

## Symptom
Nothing broke loudly. Whole-table sums differed by orders of magnitude between the two
chains ($167,908 vs $414M on one day), and the difference sat entirely at one address.

## Root cause
A transfer-accumulated ledger books a mint as a transfer *from* the zero address, so `0x0`
accumulates minus the minted supply and the table sums to zero by construction. A state
census reads `balanceOf(0x0)`, the real burned amount, positive. Every consumer's
`balance > 0` filter had been excluding the negative row without anyone intending it.

## Forbidden action
Never repoint a consumer between balance derivations without deciding where the zero
address lives. Here it is excluded at the source; do not reintroduce a zero-address row in
the balance models, and read the burned amount from the supply model's `supply_total`.

## Detection
Compare the two chains per token with and without the zero address; a sum that agrees only
after exclusion is this class. On the census side `sum(balance)` per token-day equals
`supply_holders`.

## Remediated
Source filter in place (2026-09-16); documented in the census balances model's contract and
in models/rpc_state_indexer/AGENTS.md invariant 2.
