---
id: event-field-can-lie
title: A contract's own event can report the wrong value — ground-truth a decoded field against transfers before building on it
status: documented
scope: any model built on decoded contract events where a field is taken at face value —
  models/celo/contracts/contracts_celo_gpay_settlement_events.sql and anything downstream
  of it; by extension every decode_logs model that exposes a token, amount or counterparty
symptom: a decoded field is internally consistent and looks entirely plausible, but an
  aggregate built on it disagrees with the chain — value conserves at one grain and not at
  another, or one dimension holds 100 percent of the volume when reality is split
last_verified: 2026-08-06
evidence:
  - "settlement_legacy (0xc4df5cac…) emits TokenPullSuccess with token = USDC on all 1,752 of its pulls. Raw ERC-20 Transfer logs into that contract show 345 USDC transfers totalling 17,927,696,109 and 1,407 USDT transfers totalling 58,034,396,265"
  - "the two decompose exactly: 345 + 1,407 = 1,752 events, and 17,927,696,109 + 58,034,396,265 = 75,962,092,374, which is precisely what the events attribute to USDC alone. Counts and amounts are correct; only the token field is wrong"
  - "it fails silently because the wrong value is a CONSTANT, not garbage. A misaligned ABI decode yields addresses that look like nonsense and gets caught immediately; a field that always reads USDC looks like a contract that only ever handled USDC"
  - "OUR DECODE IS NOT AT FAULT, checked because indexed flags do not affect topic0 and so a wrong flag would decode 'successfully' from the wrong bytes. The ABI declares rolesModifier and token indexed with amount in data; the raw logs carry exactly topic1, topic2 and one 64-char data word, which matches. topic2 IS the token field and it holds USDC in all 1,758 legacy events and USDT in none. The contract writes the constant itself"
  - "independently re-verified 2026-08-06 against a public Celo node (rpc.ankr.com/celo, chain 42220) with no involvement from our warehouse — eth_getTransactionReceipt plus eth_call to each token's own symbol(). Test tx 0x7b84681b… block 73731678: the bridge's event names 0xceba9300… (symbol USDC) for 13,070,000 while the only transfer into the bridge is from 0x48065fbb… (symbol USD₮) for exactly 13,070,000"
  - "two controls decode correctly through the SAME code path, which is what excludes a decoder fault: legacy tx 0x22cc85f1… where the real token IS USDC agrees, and current-contract tx 0x5c86713f… carrying USD₮ agrees. The legacy contract is right only when reality happens to match the constant it always emits"
  - "contract-level value conservation still held at 0.00, which is what made the defect survive review — the invariant as originally written summed across tokens and could not see it. Per-token it is off by 58,034,396,265 in each direction"
  - "settlement_current (0xc07cd8c2…) is unaffected: 1,087 USDC and 2,505 USDT, and per-token conservation holds exactly. Outbound events (SettlementBridged, SettlementTransferred) carry the right token on BOTH contracts"
---

## Symptom
A decoded event field looks right. It is a well-formed address, it is stable across
thousands of rows, and every aggregate built on it balances. Then a check at a different
grain disagrees with the chain: value conserves per contract but not per token, or one
category holds all the volume when you know the product handles two.

## Root cause
The contract emitted the wrong value. Not a decode fault, not an ABI mismatch — the event
itself carries a field that does not describe what happened. In this case an older
settlement contract logged a stored default token on every pull rather than the token it
actually pulled, and a later generation of the same contract fixed it.

This is invisible to every check you would normally run. The ABI matches, so decoding
succeeds. The value is a valid address, so no format check fires. It is *constant*, so it
never looks like corruption — a field that is always wrong in the same way is
indistinguishable from a contract that only ever did one thing. And an aggregate that sums
across the lying dimension still balances perfectly, which actively reassures you.

## Forbidden action
Do not treat a decoded field as authoritative because the decode succeeded. Decoding
proves the bytes matched the ABI; it proves nothing about whether the contract populated
the field correctly. In particular, do not write a per-dimension breakdown off an event
field without checking that dimension against an independent source, and do not accept a
conservation check that sums across the very dimension you are about to split by.

## Detection
Ground-truth against ERC-20 `Transfer` logs, which the token contract emits and the
settling contract cannot influence:

```sql
SELECT lower(replaceAll(address,'0x','')) AS token, count(), sum(...)
FROM celo_execution.logs
WHERE lower(replaceAll(topic0,'0x','')) = 'ddf252ad…'   -- Transfer
  AND right(lower(replaceAll(topic2,'0x','')), 40) = '<contract>'  -- topic2 = to
GROUP BY token
```

Then compare per dimension, not just in total. The decisive signal here was that event
counts and transfer counts matched exactly (1,752 = 345 + 1,407) while their token split
did not — same rows, same amounts, different labels, which can only be a labelling fault.

Note that topics in `celo_execution.logs` are stored WITHOUT a `0x` prefix. A filter
written with the prefix silently returns zero rows and reads as "no such transfers",
which is its own trap.

## Safe remediation
Take each field from the source that actually knows it. Amounts and counts from the
event, token identity from the transfer. Where a split cannot be trusted, do not offer it
— `fct_celo_gpay_settlement_batches` deliberately exposes a token-agnostic
`charged_amount` rather than a per-token breakdown that would be wrong for one contract,
and documents why in the model header.

Check the invariant at the grain you intend to publish. The contract-level conservation
check passed throughout and was the reason nobody looked; adding the per-token version is
what exposed the defect.

## Ground truth
Logs emitted by a different contract than the one under investigation. A token's own
`Transfer` event is authoritative about that token in a way the spending contract's
bespoke event never is, because the token contract has no reason to be wrong about
itself and no ability to be overridden by the caller.

## Enforcement
Documented as the `legacy-mislabels-the-pulled-token` invariant on
contracts_celo_gpay_settlement_events, and structurally avoided in
fct_celo_gpay_settlement_batches, which sources charge amounts token-agnostically. No dbt
test: the defect is in an immutable deployed contract and will never self-correct, so a
test would fail forever and be muted. The protection is that the affected breakdown is
not offered at all.
