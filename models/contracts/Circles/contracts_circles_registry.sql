{{
    config(
        materialized='view',
        tags=['dev', 'contracts', 'circles', 'registry']
    )
}}

WITH static_registry AS (
    SELECT
        lower(address) AS address,
        contract_type,
        lower(abi_source_address) AS abi_source_address,
        toUInt8(is_dynamic) AS is_dynamic,
        start_blocktime,
        discovery_source
    FROM {{ ref('contracts_circles_registry_static') }}
),
base_group_runtime AS (
    SELECT
        lower(decoded_params['group']) AS address,
        'BaseGroupRuntime' AS contract_type,
        lower('{{ var("circles_base_group_abi_source_address", "0x6fa6b486b2206ec91f9bf36ef139ebd8e4477fac") }}') AS abi_source_address,
        toUInt8(1) AS is_dynamic,
        '2024-10-01' AS start_blocktime,
        'BaseGroupFactory.BaseGroupCreated' AS discovery_source
    FROM {{ ref('contracts_circles_v2_BaseGroupFactory_events') }}
    WHERE event_name = 'BaseGroupCreated'
    GROUP BY 1
),
cm_group_runtime AS (
    SELECT
        lower(decoded_params['proxy']) AS address,
        'BaseGroupRuntime' AS contract_type,
        lower('{{ var("circles_cm_group_abi_source_address", "0x6fa6b486b2206ec91f9bf36ef139ebd8e4477fac") }}') AS abi_source_address,
        toUInt8(1) AS is_dynamic,
        '2024-10-01' AS start_blocktime,
        'CMGroupDeployer.CMGroupCreated' AS discovery_source
    FROM {{ ref('contracts_circles_v2_CMGroupDeployer_events') }}
    WHERE event_name = 'CMGroupCreated'
    GROUP BY 1
),
wrappers AS (
    SELECT
        lower(decoded_params['erc20Wrapper']) AS address,
        'ERC20Wrapper' AS contract_type,
        '' AS abi_source_address,
        toUInt8(1) AS is_dynamic,
        '2024-10-01' AS start_blocktime,
        'ERC20Lift.ERC20WrapperDeployed' AS discovery_source
    FROM {{ ref('contracts_circles_v2_ERC20Lift_events') }}
    WHERE event_name = 'ERC20WrapperDeployed'
    GROUP BY 1
),
gateways AS (
    SELECT
        lower(decoded_params['gateway']) AS address,
        'PaymentGatewayRuntime' AS contract_type,
        lower('0x590bb9934d9fb597c7d3783fe7afa635049d0bdf') AS abi_source_address,
        toUInt8(1) AS is_dynamic,
        '2024-10-01' AS start_blocktime,
        'PaymentGatewayFactory.GatewayCreated' AS discovery_source
    FROM {{ ref('contracts_circles_v2_PaymentGatewayFactory_events') }}
    WHERE event_name = 'GatewayCreated'
    GROUP BY 1
)

SELECT * FROM static_registry
UNION ALL
SELECT * FROM base_group_runtime
UNION ALL
SELECT * FROM cm_group_runtime
UNION ALL
SELECT * FROM wrappers
UNION ALL
SELECT * FROM gateways
