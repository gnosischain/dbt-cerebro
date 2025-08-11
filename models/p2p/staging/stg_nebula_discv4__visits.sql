{{ 
    config(
        pre_hook=[
          "SET allow_experimental_json_type = 1"
        ]
    )
}}

WITH

source AS (
  SELECT 
    crawl_id,
    peer_id,
    agent_version,
    protocols,
    dial_maddrs,
    filtered_maddrs,
    extra_maddrs,
    dial_errors,
    connect_maddr,
    crawl_error,
    visit_started_at,
    visit_ended_at,
    peer_properties
  FROM {{ source('nebula_discv4','visits') }} 
)

SELECT * FROM source



