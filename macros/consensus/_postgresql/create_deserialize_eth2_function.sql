-- models/_postgresql/create_deserialize_eth2_function.sql

{% macro create_deserialize_eth2_function() %}

CREATE OR REPLACE FUNCTION deserialize_eth2(eth2 bytea)
RETURNS TABLE (
    fork_digest text,
    next_fork_version text,
    next_fork_epoch numeric
) AS $$
BEGIN
    RETURN QUERY SELECT
        encode(substring(eth2 from 1 for 4), 'hex') as fork_digest,
        encode(substring(eth2 from 5 for 4), 'hex') as next_fork_version,
        bytea_to_numeric(substring(eth2 from 9 for 8)) as next_fork_epoch;
END;
$$ LANGUAGE plpgsql;

{% endmacro %}