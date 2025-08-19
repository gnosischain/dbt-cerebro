SELECT
    date
    ,cnt AS value
FROM {{ ref('int_consensus_blob_commitments_daily') }}
ORDER BY date
