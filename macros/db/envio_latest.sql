{#
  envio_latest — the ONE dedup macro for the envio_ga indexer source.

  Why not `dedup_source`: that macro orders by `insert_version DESC` (a column
  envio_ga does NOT have) and has no `_deleted` branch, so it would resurrect
  tombstoned rows. Validated 2026-06-30: envio_ga footer is
  `_deleted, _seen_version, ingested_at, _synced_block` — `_seen_version` is a
  per-table constant, so `_synced_block` is the only correct version key.

  Behaviour:
    * one row per `key`, taking the latest live value of each `col` via
      argMax(col, version) — NEVER `FINAL`.
    * tombstone guard `HAVING max(_deleted) = 0` is ORDER-INDEPENDENT: it drops
      any key that was tombstoned at ANY block (the one live tombstone,
      investment_account id='zz_fake_del_test', sits at _synced_block=0, the
      MINIMUM — so argMax(_deleted, _synced_block)=0 would wrongly keep it).
    * `version` defaults to `_synced_block`; pass a tuple (e.g.
      `(_synced_block, ingested_at)`) where the block ties per key and a
      deterministic tiebreak is required (e.g. avatar.avatar_type).
    * `pre_filter` (SQL WHERE body, no WHERE keyword) runs BEFORE the GROUP BY
      to enable partition pruning on the large stretch tables — omit it for the
      small entity tables.

  Args:
    source_name : dbt source name (always 'envio_ga')
    table_name  : source table name
    cols        : list of column names to carry (live value via argMax)
    key         : dedup grain (default 'id')
    version     : argMax ordering expression (default '_synced_block')
    pre_filter  : optional pre-aggregation WHERE body for partition pruning
#}
{% macro envio_latest(source_name, table_name, cols, key='id', version='_synced_block', pre_filter=none) %}
SELECT
    {{ key }} AS {{ key }}
    {%- for c in cols %},
    argMax({{ c }}, {{ version }}) AS {{ c }}
    {%- endfor %}
FROM {{ source(source_name, table_name) }}
{%- if pre_filter is not none and pre_filter | trim != '' %}
WHERE {{ pre_filter }}
{%- endif %}
GROUP BY {{ key }}
HAVING max(_deleted) = 0
{% endmacro %}
