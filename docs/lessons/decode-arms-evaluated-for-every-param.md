---
id: decode-arms-evaluated-for-every-param
title: >-
  A decode macro's per-type arms run for every param, so a garbage offset word in
  another type's arm can overflow substring and kill the slice
status: observed
scope: >-
  macros/decoding/decode_calls.sql (every contracts_*_calls model); the same
  evaluate-every-arm behaviour applies to any if/multiIf decode tree in ClickHouse
symptom: >-
  a decode model fails deterministically on one microbatch slice with
  "Code: 69. DB::Exception: Overflow in length argument of substring-like function:
  -9223372036854775808" while neighbouring slices succeed; the watermark stops at
  the last good slice and the runner re-tries the same slice every day
last_verified: 2026-09-07
evidence:
  - 'elementary.dbt_run_results contracts_Seer_MarketFactory_calls: 5 Code 69 errors 2026-09-03..09-07 (slices incremental_end_date=2026-09-02 and the plain run), target max(block_timestamp) frozen at 2026-08-12 01:43:35'
  - 'offending input: execution.transactions 0x18efa37b15c1ef7cc1aa3095028230a254d1408f3c193226f0339609b4bd413b (2026-09-02 16:03:40, createCategoricalMarket, single tuple param with string/string[] components, 88 calldata words, well-formed)'
  - 'reproduction: the compiled SELECT succeeds by default but throws the identical Code 69 with short_circuit_function_evaluation=disable, and via INSERT INTO FUNCTION null(...) SELECT under the dbt session settings (allow_experimental_analyzer=0, compatibility=24.6) — the deployed insert path evaluates every arm'
  - 'mechanism: the string[] arm reads the tuple offset word (0x20) as an array offset, the tuple''s first word (a string offset, 0x1c0) as the array length, then string bytes as element offsets/lengths; toUInt64(word)*2 wraps to 2^63 and substring rejects the Int64 length'
  - 'fix in tree, pending deploy: macros/decoding/decode_calls.sql — all 26 toUInt64(reinterpretAsUInt256(reverse(unhex(...)))) offset/length reads in the decode_calls body are wrapped in least(..., toUInt64(length(args_raw_hex))); verified decoded_input byte-identical pre/post for the Seer tx and for contracts_circles_v2_Hub_calls (Aug 2026) and contracts_circles_v2_NameRegistry_calls (Jun–Jul 2026) under short_circuit_function_evaluation=disable'
  - 'prior instance of the same class: commit 0bd7821f (2026-06-17) capped the dynamic range() length for approve(spender,0) misreads (ARGUMENT_OUT_OF_BOUND)'
---

## Symptom
One microbatch slice of a `contracts_*_calls` model fails every day with Code 69
"Overflow in length argument of substring-like function" while the slices around it
pass. Because the append watermark never advances past the slice, every later day
re-fails on the same transaction and the model silently stops decoding.

## Root cause
The decode tree is `if(type = 'tuple', …, if(endsWith(type,'[]'), <array arms>, <scalar
arms>))` inside an `arrayMap` over params. ClickHouse evaluates every arm for every
param on the deployed path (old-analyzer `INSERT … SELECT`; reproducible with
`short_circuit_function_evaluation=disable`). For a param that is *not* a `string[]`,
the `string[]` arm still reads its head word as an array offset, the word there as an
array length, and the following words as element offsets and lengths. Those words are
arbitrary calldata (for a tuple: inner offsets and UTF-8 string bytes). A word whose low
64 bits are ≥ 2^62 doubles to ≥ 2^63, which `substring` rejects as an Int64 length.
The earlier `range()` cap (commit 0bd7821f) bounded only the element count, not the
offsets and lengths.

## Forbidden action
Do not filter the transaction away or skip the slice; the calldata is valid and the
tuple arm decodes it correctly. Do not rely on short-circuit evaluation to protect an
arm — it is not guaranteed on every execution path.

## Detection
Elementary `dbt_run_results` with Code 69 on a decode model and a frozen
`max(block_timestamp)`; reproduce any compiled decode SELECT with
`SETTINGS short_circuit_function_evaluation='disable'` — it must not throw.

## Safe remediation
Make every arm a total function of the calldata: clamp each offset/length word to
`length(args_raw_hex)` before multiplying (a real ABI offset or length is always below
the calldata length, so valid decodes are unchanged), and keep the `range()` cap. Then
re-run the model plainly; the daily path appends everything above the watermark.

## Ground truth
The raw `execution.transactions.input` for the failing hash, decoded with an external
ABI decoder, versus the model's `decoded_input`.

## Enforcement
None yet. Candidate: a compile-time smoke test that runs each decode macro's compiled
SELECT over a fixed set of awkward calldata rows with
`short_circuit_function_evaluation='disable'`.
