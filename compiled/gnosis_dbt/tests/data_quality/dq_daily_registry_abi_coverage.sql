
-- Structural invariant for proxy registries decoded through decode_logs with
-- contract_address_ref: every mastercopy a registry points at (abi_source_address)
-- must have at least one event_signatures row on that chain. decode_logs resolves the
-- ABI by abi_source_address, so a registry that gains a new mastercopy whose ABI was
-- never seeded decodes every proxy of that generation to NOTHING — zero rows, green
-- runs, watermark frozen at the migration. First instance: the June 2026 GP Safe
-- migration added Roles mastercopy 0x732b9e9f... (35,743 proxies) to
-- contracts_gpay_modules_registry while event_signatures only carried the old
-- 0x9646fdad...; int_execution_gpay_roles_events froze at 2026-06-03 for three months
-- (docs/lessons/stale-seed-deploy-silent-decode-halt.md). Severity is error on
-- purpose: a hit here guarantees a silently frozen decode, there is no benign case.
WITH mastercopies AS (
    SELECT
        'contracts_gpay_modules_registry'                        AS registry,
        contract_type,
        lower(replaceAll(abi_source_address, '0x', ''))          AS mastercopy,
        count()                                                  AS proxies
    FROM `dbt`.`contracts_gpay_modules_registry`
    WHERE abi_source_address IS NOT NULL AND abi_source_address != ''
    GROUP BY contract_type, mastercopy
),
seeded AS (
    SELECT DISTINCT lower(replaceAll(contract_address, '0x', '')) AS mastercopy
    FROM `dbt`.`event_signatures`
    WHERE chain = 'gnosis'
)
SELECT m.registry, m.contract_type, concat('0x', m.mastercopy) AS mastercopy, m.proxies
FROM mastercopies m
WHERE m.mastercopy NOT IN (SELECT mastercopy FROM seeded)
ORDER BY m.proxies DESC