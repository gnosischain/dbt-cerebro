{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

SELECT
    id,
    space_id,
    -- GIP identity from a leading title match ("GIP-151: ...", "[Redo] GIP-87").
    -- NULL for non-GIP proposals and for titles that only mention a GIP.
    {{ parse_gip_number('title') }} AS gip_number,
    title,
    state,
    type,
    lower(author)                                        AS author,
    created_at,
    start_at,
    end_at,
    snapshot_block,
    scores_total,
    quorum,
    votes_count,
    scores_state,
    -- Positionally aligned: scores[i] is the score for choices[i].
    JSONExtract(raw_json, 'choices', 'Array(String)')    AS choices,
    JSONExtract(raw_json, 'scores',  'Array(Float64)')   AS scores,
    -- Per-proposal strategy set (positionally aligned to a vote's
    -- vp_by_strategy). Empty until proposals are re-ingested with the
    -- `strategies` field. Used by the power-source split.
    arrayMap(x -> JSONExtractString(x, 'name'),    JSONExtractArrayRaw(raw_json, 'strategies')) AS strategy_names,
    arrayMap(x -> JSONExtractString(x, 'network'), JSONExtractArrayRaw(raw_json, 'strategies')) AS strategy_networks,
    JSONExtractString(raw_json, 'body')                  AS body,
    JSONExtractString(raw_json, 'link')                  AS link,
    ingested_at
FROM {{ source('governance', 'snapshot_proposals') }} FINAL
