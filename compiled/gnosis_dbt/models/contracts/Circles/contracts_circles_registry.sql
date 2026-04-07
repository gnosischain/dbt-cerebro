



-- depends_on: `dbt`.`contracts_factory_registry`
-- depends_on: `dbt`.`contracts_circles_v2_BaseGroupFactory_events`
-- depends_on: `dbt`.`contracts_circles_v2_CMGroupDeployer_events`
-- depends_on: `dbt`.`contracts_circles_v2_ERC20Lift_events`
-- depends_on: `dbt`.`contracts_circles_v2_PaymentGatewayFactory_events`
-- depends_on: `dbt`.`contracts_circles_v2_ERC20TokenOfferFactory_events`
-- depends_on: `dbt`.`contracts_circles_v2_CirclesBackingFactory_events`

WITH static_registry AS (
    SELECT
        lower(address) AS address,
        contract_type,
        lower(abi_source_address) AS abi_source_address,
        toUInt8(is_dynamic) AS is_dynamic,
        start_blocktime,
        discovery_source
    FROM `dbt`.`contracts_circles_registry_static`
)

SELECT * FROM static_registry
UNION ALL







  
    
    SELECT
      lower(decoded_params['gateway']) AS address,
      'PaymentGatewayRuntime' AS contract_type,
      lower('0x590bb9934d9fb597c7d3783fe7afa635049d0bdf') AS abi_source_address,
      toUInt8(1) AS is_dynamic,
      '2025-12-01' AS start_blocktime,
      'GatewayCreated' AS discovery_source
    FROM `dbt`.`contracts_circles_v2_PaymentGatewayFactory_events`
    WHERE event_name = 'GatewayCreated'
    GROUP BY 1
  
    
    UNION ALL
    
    SELECT
      lower(decoded_params['tokenOffer']) AS address,
      'ERC20TokenOfferRuntime' AS contract_type,
      lower('0x12dfed45783d61c093706a6567404a9d3ab3d1b8') AS abi_source_address,
      toUInt8(1) AS is_dynamic,
      '2025-08-01' AS start_blocktime,
      'ERC20TokenOfferCreated' AS discovery_source
    FROM `dbt`.`contracts_circles_v2_ERC20TokenOfferFactory_events`
    WHERE event_name = 'ERC20TokenOfferCreated'
    GROUP BY 1
  
    
    UNION ALL
    
    SELECT
      lower(decoded_params['offerCycle']) AS address,
      'ERC20TokenOfferCycleRuntime' AS contract_type,
      lower('0x76a42aebb2c54d7e259b1c7e4eb0cadf5897a7de') AS abi_source_address,
      toUInt8(1) AS is_dynamic,
      '2025-08-01' AS start_blocktime,
      'ERC20TokenOfferCycleCreated' AS discovery_source
    FROM `dbt`.`contracts_circles_v2_ERC20TokenOfferFactory_events`
    WHERE event_name = 'ERC20TokenOfferCycleCreated'
    GROUP BY 1
  
    
    UNION ALL
    
    SELECT
      lower(decoded_params['erc20Wrapper']) AS address,
      'ERC20Wrapper' AS contract_type,
      lower('') AS abi_source_address,
      toUInt8(1) AS is_dynamic,
      '2024-10-01' AS start_blocktime,
      'ERC20WrapperDeployed' AS discovery_source
    FROM `dbt`.`contracts_circles_v2_ERC20Lift_events`
    WHERE event_name = 'ERC20WrapperDeployed'
    GROUP BY 1
  
    
    UNION ALL
    
    SELECT
      lower(decoded_params['group']) AS address,
      'BaseGroupRuntime' AS contract_type,
      lower('0x6fa6b486b2206ec91f9bf36ef139ebd8e4477fac') AS abi_source_address,
      toUInt8(1) AS is_dynamic,
      '2025-04-01' AS start_blocktime,
      'BaseGroupCreated' AS discovery_source
    FROM `dbt`.`contracts_circles_v2_BaseGroupFactory_events`
    WHERE event_name = 'BaseGroupCreated'
    GROUP BY 1
  
    
    UNION ALL
    
    SELECT
      lower(decoded_params['circlesBackingInstance']) AS address,
      'CirclesBackingOrderRuntime' AS contract_type,
      lower('0x43866c5602b0e3b3272424396e88b849796dc608') AS abi_source_address,
      toUInt8(1) AS is_dynamic,
      '2025-04-01' AS start_blocktime,
      'CirclesBackingDeployed' AS discovery_source
    FROM `dbt`.`contracts_circles_v2_CirclesBackingFactory_events`
    WHERE event_name = 'CirclesBackingDeployed'
    GROUP BY 1
  
    
    UNION ALL
    
    SELECT
      lower(decoded_params['proxy']) AS address,
      'BaseGroupRuntime' AS contract_type,
      lower('0x6fa6b486b2206ec91f9bf36ef139ebd8e4477fac') AS abi_source_address,
      toUInt8(1) AS is_dynamic,
      '2025-02-01' AS start_blocktime,
      'CMGroupCreated' AS discovery_source
    FROM `dbt`.`contracts_circles_v2_CMGroupDeployer_events`
    WHERE event_name = 'CMGroupCreated'
    GROUP BY 1
  


