{% macro compute_timestamp_at_slot(slot) %}

addSeconds(
    (SELECT f_time FROM {{ get_postgres('gnosis_chaind', 't_genesis') }} LIMIT 1),
    ({{ slot }} - 0) * (SELECT toInt32(f_value) FROM {{ get_postgres('gnosis_chaind', 't_chain_spec') }} WHERE f_key = 'SECONDS_PER_SLOT' LIMIT 1)
)

{% endmacro %}


{% macro compute_timestamp_at_epoch(epoch) %}

addSeconds(
    (SELECT f_time FROM {{ get_postgres('gnosis_chaind', 't_genesis') }} LIMIT 1),
    ({{ epoch }} - 0) * (SELECT toInt32(f_value) FROM {{ get_postgres('gnosis_chaind', 't_chain_spec') }} WHERE f_key = 'SECONDS_PER_SLOT' LIMIT 1)
    *(SELECT toInt32(f_value) FROM {{ get_postgres('gnosis_chaind', 't_chain_spec') }} WHERE f_key = 'SLOTS_PER_EPOCH' LIMIT 1)
)

{% endmacro %}