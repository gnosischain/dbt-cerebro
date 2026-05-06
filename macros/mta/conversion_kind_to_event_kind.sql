{#
  conversion_kind_to_event_kind(expr)

  Returns the `event_kind` that corresponds to the same observed event as
  the given `conversion_kind`. Used by journey-spine materializations to
  exclude the conversion's own kind from the touchpoint set (leakage
  guard).

  Mirrors the seed `mta_conversion_to_event_kind.csv` (GA-side) and
  `mta_gp_conversion_to_event_kind.csv` (GP-side). When you add a new
  conversion_kind, update both the seed AND this macro.

  Input:  any SQL expression that evaluates to LowCardinality(String) /
          String — typically `c.conversion_kind`.
  Output: ClickHouse multiIf chain returning event_kind String.
#}
{% macro conversion_kind_to_event_kind(expr) %}
  multiIf(
    {{ expr }} = 'topup',                 'chain.topup',
    {{ expr }} = 'swap_filled',           'chain.swap_filled',
    {{ expr }} = 'token_offer_claim',     'chain.token_offer_claim',
    {{ expr }} = 'marketplace_buy',       'chain.marketplace_buy',
    {{ expr }} = 'gpay_payment',          'gp.payment',
    {{ expr }} = 'gpay_funded',           'gp.deposit',
    {{ expr }} = 'gpay_cashback_claim',   'gp.cashback_claim',
    'unknown'
  )
{% endmacro %}
