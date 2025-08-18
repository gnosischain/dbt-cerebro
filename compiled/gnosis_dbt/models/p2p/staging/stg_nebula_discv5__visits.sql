

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
  FROM `nebula`.`visits` 
)

SELECT * FROM source