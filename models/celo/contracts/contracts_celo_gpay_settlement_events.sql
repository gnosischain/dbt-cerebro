{{
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'append',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(block_timestamp, log_index)',
        unique_key              = '(block_timestamp, log_index)',
        partition_by            = 'toStartOfMonth(block_timestamp)',
        settings                = {
                                    'allow_nullable_key': 1
                                },
        tags                    = ['production','celo','gpay','contracts','settlement','events','microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
-- Gnosis Pay's own settlement-contract event stream on Celo — the authoritative
-- record of what the card platform did, as opposed to int_celo_gpay_activity,
-- which INFERS the same events from ERC-20 Transfer direction.
--
-- Both contracts in one model on purpose. decode_logs joins a log to its ABI on
-- (address, topic0), not topic0 alone, so two contracts with disjoint signatures
-- decode side by side without any chance of cross-matching. The address set comes
-- from the seed rather than a literal list because keying this tree on a single
-- hardcoded bridge is exactly what hid 235 cards and 1,743 payments until
-- 2026-08-05 (docs/lessons/circular-completeness-proof.md). Add a bridge to the
-- seed and it decodes here automatically.
--
-- THE ONE RULE: adding a row to celo_gpay_settlement_contracts REQUIRES fetching
-- that contract's ABI in the same change:
--     python scripts/signatures/fetch_abi_to_csv.py 0xNEW --chain celo --regen
-- Without it the address is scanned but matches no signature, so its logs land
-- with event_name = NULL instead of being dropped. That is deliberate — a visible
-- null beats a silently absent contract — and it is the probe in the schema's
-- `no-undecoded-settlement-logs` invariant.
--
-- The two ABIs share zero topic0s despite being the same contract design: every
-- event on settlement_current carries an extra indexed `sender` as its first
-- param, which changes the signature and therefore the hash. Same function
-- selectors on both (settle, USDC, USDT, USDT_OFT, quoteNativeBridgeFee).
--
-- Events, and what each is good for:
--   TokenPullSuccess           per-payment charge. Carries the card's Roles module
--                              (`rolesModifier`), so it joins to a card without
--                              going through transfers at all. Reconciles EXACTLY
--                              with int_celo_gpay_activity Payment rows — 5,165 v
--                              5,165 on 2026-08-05, and per-day zero variance on
--                              both contracts independently.
--   TokenPullFailed /          charge attempts that did NOT move money. Invisible
--   TokenPullFailedWithAmount  to the transfer-based model by construction. 7 so
--                              far (5 legacy, 2 current); TokenPullFailed with its
--                              errorData payload has never fired.
--   SettlementBridged          treasury leg, cross-chain via the LayerZero USDT0
--                              OFT — carries destinationEid and refundAddress.
--   SettlementTransferred      treasury leg, settled locally on Celo instead.
--   NativeBridgeFeePaid        CELO paid to bridge a batch. The cost side of
--                              settlement, currently unmeasured anywhere.
--   BridgeMessageReceived /    LayerZero receipts (GUID, nonce, fees).
--   BridgeOftReceiptReceived   settlement_current only.
--
-- start_blocktime is the first legacy event (2026-03-31 14:16:56), ~5 days before
-- the first card was provisioned. Nothing GP-related on either contract precedes
-- it, so there is no reason to scan back to the L2 migration.
--
-- HEAVY on first build: a full-history scan of celo_execution.logs in one query
-- OOMs (ClickHouse Code 241). Rebuild through scripts/full_refresh/refresh.py in
-- the monthly batches declared in schema.yml, never a plain --full-refresh.
{{
    decode_logs(
        source_table         = source('celo_execution','logs'),
        contract_address_ref = ref('celo_gpay_settlement_contracts'),
        output_json_type     = true,
        incremental_column   = 'block_timestamp',
        start_blocktime      = '2026-03-31',
        chain                = 'celo'
    )
}}
