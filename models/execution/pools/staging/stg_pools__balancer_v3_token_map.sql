{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'balancer_v3', 'staging']
    )
}}

SELECT '0x773cda0cade2a3d86e6d4e30699d40bb95174ff2' AS wrapper_address, 'waGnowstETH' AS wrapper_symbol, toUInt8(18) AS wrapper_decimals, '0x6c76971f98945ae98dd7d4dfca8711ebea946ea6' AS underlying_address
UNION ALL
SELECT '0x57f664882f762fa37903fc864e2b633d384b411a', 'waGnoWETH', toUInt8(18), '0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1'
UNION ALL
SELECT '0x51350d88c1bd32cc6a79368c9fb70373fb71f375', 'waGnoUSDCe', toUInt8(6), '0x2a22f9c3b484c3629090feed35f17ff8f88f76f0'
UNION ALL
SELECT '0x7c16f0185a26db0ae7a9377f23bc18ea7ce5d644', 'waGnoGNO', toUInt8(18), '0x9c58bacc331c9aa871afd802db6379a98e80cedb'
UNION ALL
SELECT '0x58d9acac48a4077e4909181c48decd00e5ba5de4', 'waGnoGHO', toUInt8(18), '0xfc421ad3c883bf9e7c4f42de845c4e4405799e73'
