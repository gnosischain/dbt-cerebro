{#
  Generic test: TokenPullSuccess count == inferred Payment count, bound at
  least(act_wm, pull_wm). Declared on contracts_celo_gpay_settlement_events in
  models/celo/contracts/schema.yml — lives here (not tests/) because it is a
  Celo project invariant and needs ref() in the SQL.
  Returns one diagnostic row on mismatch; zero rows => pass.
#}
{% test celo_gpay_charges_match_payments(model) %}

WITH bound AS (
    SELECT least(
        (SELECT max(block_time) FROM {{ ref('int_celo_gpay_activity') }}),
        (SELECT max(block_timestamp)
         FROM {{ model }}
         WHERE event_name = 'TokenPullSuccess')
    ) AS shared_wm
),

counts AS (
    SELECT
        (SELECT count()
         FROM {{ model }}
         WHERE event_name = 'TokenPullSuccess'
           AND block_timestamp <= (SELECT shared_wm FROM bound)) AS pulls,
        (SELECT count()
         FROM {{ ref('int_celo_gpay_activity') }}
         WHERE action = 'Payment'
           AND block_time <= (SELECT shared_wm FROM bound)) AS payments,
        (SELECT shared_wm FROM bound) AS shared_wm
)

SELECT
    payments,
    pulls,
    payments - pulls AS delta,
    shared_wm
FROM counts
WHERE payments != pulls

{% endtest %}
