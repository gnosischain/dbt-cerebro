{# ================================================================
   create_circles_avatar_metadata_table.sql

   One-shot DDL macro that creates the raw landing table for fetched
   Circles avatar IPFS metadata. Mirrors how `contracts_abi` is
   provisioned: a plain ClickHouse table outside the dbt model graph,
   written by both the one-time Python backfill and the nightly
   `fetch_and_insert_circles_metadata` run-operation.

   Usage:
     dbt run-operation create_circles_avatar_metadata_table
================================================================ #}

{% macro create_circles_avatar_metadata_table() %}
  {% set sql %}
    CREATE TABLE IF NOT EXISTS circles_avatar_metadata_raw
    (
      avatar          LowCardinality(String),
      metadata_digest String,
      ipfs_cid_v0     String,
      gateway_url     String,
      http_status     UInt16,
      content_type    String,
      body            String,
      error           String,
      fetched_at      DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(fetched_at)
    ORDER BY (metadata_digest, avatar)
  {% endset %}

  {{ log("Creating circles_avatar_metadata_raw if not exists", info=true) }}
  {% do run_query(sql) %}
{% endmacro %}
