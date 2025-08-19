{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, graffiti)',
        unique_key='(date, graffiti)',
        partition_by='toStartOfMonth(date)'
    )
}}

WITH
    -- Canonical lists
    ['nethermind','erigon','geth','besu','reth'] AS el_keys,
    ['Nethermind','Erigon','Geth','Besu','Reth'] AS el_names,
    ['lighthouse','teku','prysm','lodestar','nimbus'] AS cl_keys,
    ['Lighthouse','Teku','Prysm','Lodestar','Nimbus'] AS cl_names,

    -- Platform/hosting brands 
    ['dappnode','avado','allnodes','twinstake','stakewise','gateway','kleros',
     'filoozom','kpk-validators','hopr','digitalconsultantsllc',
     'synthex'] AS brand_keys,
    ['DappNode','Avado','Allnodes','Twinstake','StakeWise','gateway.fm','kleros.io',
     'filoozom.eth','kpk-validators','HOPR','DigitalConsultantsLLC.xyz',
     'Synthex'] AS brand_names,


final AS (
    SELECT
        date
        ,graffiti
        ,cnt
        -- Lowercased text to search in
        ,lowerUTF8(graffiti) AS g

        -- Detect any brand first (highest precedence)
        ,arrayFilter(x -> positionCaseInsensitive(g, x) > 0, brand_keys) AS brands_found
        ,if(length(brands_found) > 0,
            arrayElement(brand_names, indexOf(brand_keys, brands_found[1])),
            null) AS brand_label

        -- Detect EL/CL clients (order-insensitive, separator-agnostic)
        ,arrayFilter(x -> positionCaseInsensitive(g, x) > 0, el_keys) AS el_found
        ,arrayFilter(x -> positionCaseInsensitive(g, x) > 0, cl_keys) AS cl_found

        -- Pick the first match per side by priority order above
        ,if(length(el_found) > 0,
            arrayElement(el_names, indexOf(el_keys, el_found[1])),
            null) AS el_label

        ,if(length(cl_found) > 0,
            arrayElement(cl_names, indexOf(cl_keys, cl_found[1])),
            null) AS cl_label

        -- Final label priority:
        -- 1) Brand/platform if any
        -- 2) EL+CL combo if both found
        -- 3) Single client if only one side found
        -- 4) Other
        ,coalesce(
            brand_label,
            if(el_label IS NOT NULL AND cl_label IS NOT NULL, concat(el_label, '+', cl_label), null),
            el_label,
            cl_label,
            if(graffiti = 'None', graffiti, null),
            'Other'
        ) AS label
    FROM (
        SELECT
            toStartOfDay(slot_timestamp) AS date
            ,IF(graffiti='0x0000000000000000000000000000000000000000000000000000000000000000', 
                'None', 
                unhex(right(graffiti,-2))
            ) AS graffiti
            ,COUNT(*) AS cnt
        FROM {{ ref('stg_consensus__blocks') }}
        WHERE
            slot_timestamp < today()
            {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
        GROUP BY 1, 2
    )
)

SELECT
    date
    ,graffiti
    ,label
    ,cnt
FROM final