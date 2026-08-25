

SELECT
    id,
    space_id,
    -- GIP identity from a leading title match ("GIP-151: ...", "[Redo] GIP-87").
    -- NULL for non-GIP proposals and for titles that only mention a GIP.
    
toUInt32OrNull(extract(
    title,
    '(?i)^[\\s\\x{200B}\\x{FEFF}#]*(?:\\[[^\\]]*\\]\\s*)*(?:\\([^)]*\\)\\s*)*(?:re-?do of:\\s*)?GIP\\s*-?\\s*0*([0-9]+)'
)) AS gip_number,
    title,
    state,
    type,
    lower(author)                                        AS author,
    discussion,
    -- Topic id parsed from the proposal-authored discussion URL (e.g.
    -- .../t/gip-151-.../12337, sometimes with a trailing ?u=... suffix that
    -- must be stripped before anchoring on the trailing numeric segment).
    -- Cross-checked against the forum-post-link bridge (see
    -- api_governance_forum_topic_proposal_links): of 89 that parse, 43 have
    -- NO corroborating pasted-post link at all — a genuine additional
    -- coverage source, not merely a corroboration signal on top of the
    -- post-link bridge. NULL when discussion is empty, points elsewhere (e.g.
    -- another proposal, an unrelated domain), or has no numeric topic segment
    -- (a handful of forum links are slug-only).
    CASE WHEN discussion LIKE '%forum.gnosis.io%'
         THEN toUInt32OrNull(extract(splitByChar('?', discussion)[1], '/([0-9]+)/?$'))
         ELSE NULL
    END                                                   AS discussion_topic_id,
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
FROM `governance_db`.`snapshot_proposals` FINAL