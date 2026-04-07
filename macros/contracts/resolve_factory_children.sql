{# ================================================================
   resolve_factory_children.sql - Generic Factory Contract Discovery

   This macro generates SQL to discover child contracts created by
   factory contracts. It reads from the contracts_factory_registry
   seed table and generates UNION ALL queries that extract child
   contract addresses from factory creation events.

   Purpose:
   - Replaces hand-coded factory discovery CTEs in protocol registries
   - Any protocol can declare factory->child relationships via CSV
   - Generates consistent discovery SQL at compile time

   Parameters:
   - protocol: Optional protocol filter (e.g., 'circles', 'uniswapv3')
               If null, returns all factory children across all protocols

   Output columns (matching static registry schema):
   - address: The discovered child contract address
   - contract_type: The type assigned to the child contract
   - abi_source_address: Address to use for ABI resolution
   - is_dynamic: Always 1 (dynamically discovered)
   - start_blocktime: From the factory registry config
   - discovery_source: The creation event name used for discovery

   Usage:
   SELECT * FROM static_registry
   UNION ALL
   {{ resolve_factory_children(protocol='circles') }}
================================================================ #}

{% macro resolve_factory_children(protocol=none) %}

{% set factory_query %}
  SELECT
    factory_address,
    factory_events_model,
    creation_event_name,
    child_address_param,
    child_contract_type,
    child_abi_source_address,
    protocol,
    start_blocktime
  FROM {{ ref('contracts_factory_registry') }}
  {% if protocol %}
    WHERE protocol = '{{ protocol }}'
  {% endif %}
{% endset %}

{% set factories = run_query(factory_query) %}

{% if factories | length == 0 %}
  {# Return empty result set matching the expected schema #}
  SELECT
    '' AS address,
    '' AS contract_type,
    '' AS abi_source_address,
    toUInt8(1) AS is_dynamic,
    '' AS start_blocktime,
    '' AS discovery_source
  WHERE 1 = 0
{% else %}
  {% for row in factories %}
    {% if not loop.first %}
    UNION ALL
    {% endif %}
    SELECT
      lower(decoded_params['{{ row.child_address_param }}']) AS address,
      '{{ row.child_contract_type }}' AS contract_type,
      lower('{{ row.child_abi_source_address }}') AS abi_source_address,
      toUInt8(1) AS is_dynamic,
      '{{ row.start_blocktime }}' AS start_blocktime,
      '{{ row.creation_event_name }}' AS discovery_source
    FROM {{ ref(row.factory_events_model) }}
    WHERE event_name = '{{ row.creation_event_name }}'
    GROUP BY 1
  {% endfor %}
{% endif %}

{% endmacro %}
