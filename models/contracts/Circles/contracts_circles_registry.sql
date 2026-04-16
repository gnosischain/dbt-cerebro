{{
    config(
        materialized='view',
        tags=['production', 'contracts', 'circles_v2', 'registry']
    )
}}

-- depends_on: {{ ref('contracts_factory_registry') }}
-- depends_on: {{ ref('contracts_circles_v2_BaseGroupFactory_events') }}
-- depends_on: {{ ref('contracts_circles_v2_CMGroupDeployer_events') }}
-- depends_on: {{ ref('contracts_circles_v2_ERC20Lift_events') }}
-- depends_on: {{ ref('contracts_circles_v2_PaymentGatewayFactory_events') }}
-- depends_on: {{ ref('contracts_circles_v2_ERC20TokenOfferFactory_events') }}
-- depends_on: {{ ref('contracts_circles_v2_CirclesBackingFactory_events') }}

WITH static_registry AS (
    SELECT
        lower(address) AS address,
        contract_type,
        lower(abi_source_address) AS abi_source_address,
        toUInt8(is_dynamic) AS is_dynamic,
        start_blocktime,
        discovery_source
    FROM {{ ref('contracts_circles_registry_static') }}
)

SELECT * FROM static_registry
UNION ALL
{{ resolve_factory_children(protocol='circles_v2') }}
