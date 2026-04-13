{#
  pseudonymize_address(addr_expr)

  Keyed, non-reversible pseudonym for any wallet address or user
  identifier. Uses a secret salt from env_var('CEREBRO_PII_SALT')
  so the hash cannot be reversed via rainbow tables against the
  public on-chain address space.

  Use this macro on BOTH sides of any cross-domain join that
  involves wallet addresses, so raw addresses never flow through
  a materialized model.

  Input:  any SQL expression that evaluates to a String
          (raw address, distinct_id, JSONExtractString(...), etc.)
  Output: UInt64

  ─ Salt format ─────────────────────────────────────────────────
  CEREBRO_PII_SALT must be a hex-encoded string (even length, chars
  [0-9a-fA-F]).  Generate one with:
      openssl rand -hex 32
  Hex is required because the salt is embedded as a string literal
  in compiled SQL; raw bytes (quotes, non-UTF-8, etc.) would break
  the literal or the HTTP driver.  The salt is decoded to bytes
  inside ClickHouse via unhex() at query time.

  The env_var is called without a default so `dbt parse` fails
  loudly if the salt is not set.  A silent empty-string fallback
  would re-introduce the rainbow-table problem this macro exists
  to prevent.
#}
{% macro pseudonymize_address(addr_expr) %}
    sipHash64(concat(unhex('{{ env_var("CEREBRO_PII_SALT") }}'), lower({{ addr_expr }})))
{% endmacro %}
