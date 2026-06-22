
-- All Chainlink price-feed aggregators we use on Gnosis, decoded in a SINGLE pass.
-- One combined model (vs one per feed) so execution.logs is scanned once per month
-- instead of once per feed (~9x fewer scans; cheaper backfill + daily refresh).
-- decoded_params['current'] is the answer; contract_address identifies which feed
-- (mapped to feed -> token symbol in int_execution_prices_oracle_daily). Address arrays
-- cover both phases per feed (phaseAggregators) for full history.
--
--   GNO/USD    : 0x016a45F646bbd35B61fE7A496a75D9Ea69bD243E, 0xcA16Ed36A7d1Ae2DC68873D62bce4f9BdCc2d378
--   ETH/USD    : 0x44513922bf52cEc40a0557797b040805deD50140, 0x059e7Bd8157e0d302dF3626E162B6C835340b311
--   WBTC/USD   : 0x5ED6A59735297Bc5D6CB4942913Ae7098E0cD703
--   EUR/USD    : 0x759be90a34E426042ed7d17916B78a5cD2567dd1
--   CHF/USD    : 0xbe18b8F41760878ba6D3b1E9475c4CcAD3D9aA8f, 0x6E2482E011EC31a1960a938791B6B4Ff5BAa3217
--   wstETH-ETH : 0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55  (18-decimal exchange rate)
--   USDC/USD   : 0xc15288Bc7E921dc462d9c4CE151318D5AA428a53, 0x30bA871Ee7a08dBd255CdD8e7e035DAd72014E27
--   USDT/USD   : 0xc4D924b6baB6FEc909E482b93847D997463f0c79
--   DAI/USD    : 0x12A6B73A568f8DC3D24DA1654079343f18f69236, 0xb65566283CAcE6b281308308da0f0783a613c416




  
  
  
    
  

  
  
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  
    
    
  

  
  
    
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
    
    
  












WITH

logs AS (
  SELECT * FROM (
    SELECT *,
      row_number() OVER (
        PARTITION BY block_number, transaction_index, log_index
        ORDER BY insert_version DESC
      ) AS _dedup_rn
    FROM `execution`.`logs`
    WHERE lower(replaceAll(address, '0x', '')) IN ('016a45f646bbd35b61fe7a496a75d9ea69bd243e', 'ca16ed36a7d1ae2dc68873d62bce4f9bdcc2d378', '44513922bf52cec40a0557797b040805ded50140', '059e7bd8157e0d302df3626e162b6c835340b311', '5ed6a59735297bc5d6cb4942913ae7098e0cd703', '759be90a34e426042ed7d17916b78a5cd2567dd1', 'be18b8f41760878ba6d3b1e9475c4ccad3d9aa8f', '6e2482e011ec31a1960a938791b6b4ff5baa3217', '6dcf8ce1982fc71e7128407c7c6ce4b0c1722f55', 'c15288bc7e921dc462d9c4ce151318d5aa428a53', '30ba871ee7a08dbd255cdd8e7e035dad72014e27', 'c4d924b6bab6fec909e482b93847d997463f0c79', '12a6b73a568f8dc3d24da1654079343f18f69236', 'b65566283cace6b281308308da0f0783a613c416')

      

      
        AND block_timestamp >= toDateTime('2021-01-01')
      

      
      

      
      
        
        
          
          
          
        
        
        AND block_number > 46823709
        AND block_timestamp >= toDateTime('2026-06-22 07:13:55')
        
        
        
      
  )
  WHERE _dedup_rn = 1
),


logs_with_abi AS (
  SELECT
    l.*,
    lower(replaceAll(l.address, '0x', '')) AS abi_join_address
  FROM logs l
),


abi AS ( 
SELECT
  replaceAll(lower(contract_address), '0x', '')          AS abi_contract_address,
  replace(signature,'0x','')                     AS topic0_sig,
  event_name,
  arrayMap(x->JSONExtractString(x,'name'),
           JSONExtractArrayRaw(params))          AS names,
  arrayMap(x->JSONExtractString(x,'type'),
           JSONExtractArrayRaw(params))          AS types,
  arrayMap(x->JSONExtractBool(x,'indexed'),
           JSONExtractArrayRaw(params))          AS flags
FROM `dbt`.`event_signatures`
WHERE replaceAll(lower(contract_address),'0x','') IN ('016a45f646bbd35b61fe7a496a75d9ea69bd243e', 'ca16ed36a7d1ae2dc68873d62bce4f9bdcc2d378', '44513922bf52cec40a0557797b040805ded50140', '059e7bd8157e0d302df3626e162b6c835340b311', '5ed6a59735297bc5d6cb4942913ae7098e0cd703', '759be90a34e426042ed7d17916b78a5cd2567dd1', 'be18b8f41760878ba6d3b1e9475c4ccad3d9aa8f', '6e2482e011ec31a1960a938791b6b4ff5baa3217', '6dcf8ce1982fc71e7128407c7c6ce4b0c1722f55', 'c15288bc7e921dc462d9c4ce151318d5aa428a53', '30ba871ee7a08dbd255cdd8e7e035dad72014e27', 'c4d924b6bab6fec909e482b93847d997463f0c79', '12a6b73a568f8dc3d24da1654079343f18f69236', 'b65566283cace6b281308308da0f0783a613c416')
 ),

process AS (
  SELECT
    l.block_number,
    l.block_timestamp,
    l.transaction_hash,
    l.transaction_index,
    l.log_index,
    l.address           AS contract_address,
    a.event_name,

    -- ABI arrays
    a.names             AS param_names,
    a.types             AS param_types,
    a.flags             AS param_flags,
    length(a.types)     AS n_params,

    -- topics and data
    [l.topic1, l.topic2, l.topic3]       AS raw_topics,
    replaceAll(l.data,'0x','')           AS data_hex,

    -- non-indexed metadata (zip flags/types/positions, then filter non-indexed)
    arrayFilter((f,t,i) -> not f,
      arrayZip(a.flags, a.types, range(n_params))
    )                                    AS ni_meta,

    arrayMap(x -> x.2, ni_meta)          AS ni_types,
    arrayMap(x -> x.3, ni_meta)          AS ni_positions,

    -- head words (32-byte) from start of the data head area
    arrayMap(i ->
      if(i*64 < length(data_hex),
         substring(data_hex, 1 + i*64, 64),
         NULL),
      range(greatest(length(ni_types), 1) * 16)  -- generous upper bound
    )                                    AS data_words,

    -- base type for arrays (strip [])
    arrayMap(j -> replaceRegexpOne(ni_types[j+1], '\\[\\]$', ''), range(length(ni_types))) AS ni_base_types,

    /* ===================== DECODING ====================== */
    -- For each non-indexed param j return a STRING:
    --  - Arrays -> toJSONString(Array(String))
    --  - Dynamic scalars -> String (hex or utf8)
    --  - Static scalars -> String
    arrayMap(j ->
      if(
        /* -------- ARRAY TYPES -------- */
        endsWith(ni_types[j+1],'[]'),

        /* Build JSON string of the fully decoded array */
        toJSONString(
          arrayMap(
            k ->
              multiIf(
                ni_base_types[j+1] = 'address',
                  concat(
                    '0x',
                    substring(
                      substring(
                        data_hex,
                        1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2,
                        64 + 64 + (k + 1) * 64
                      ),
                      (64 + k*64) + 25, 40
                    )
                  ),

                ni_base_types[j+1] = 'bytes32',
                  concat(
                    '0x',
                    substring(
                      data_hex,
                      1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2 + 64 + k*64,
                      64
                    )
                  ),

                startsWith(ni_base_types[j+1], 'uint')
                OR ni_base_types[j+1] = 'bool',
                  /* bool is stored as a 0/1 uint256 word, so we can use
                     the same reinterpret path as uint*. Output is '0' or
                     '1' (decimal string). */
                  toString(
                    reinterpretAsUInt256(
                      reverse(
                        unhex(
                          substring(
                            data_hex,
                            1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2 + 64 + k*64,
                            64
                          )
                        )
                      )
                    )
                  ),

                  startsWith(ni_base_types[j+1], 'int'),
                  toString(
                    reinterpretAsInt256(
                      reverse(
                        unhex(
                          substring(
                            data_hex,
                            1 + toUInt32(reinterpretAsInt256(reverse(unhex(data_words[j+1])))) * 2 + 64 + k*64,
                            64
                          )
                        )
                      )
                    )
                  ),

                /* Fallback: full 32-byte hex */
                concat(
                  '0x',
                  substring(
                    data_hex,
                    1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2 + 64 + k*64,
                    64
                  )
                )
              ),
            /* range(N) where N is array length at base */
            range(
              toUInt32(
                reinterpretAsUInt256(
                  reverse(
                    unhex(
                      substring(
                        data_hex,
                        1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2,
                        64
                      )
                    )
                  )
                )
              )
            )
          )
        ),

        /* -------- DYNAMIC SCALARS (string/bytes/bytesN≠32) -------- */
        if(
          ni_types[j+1] = 'bytes'
          OR ni_types[j+1] = 'string'
          OR (startsWith(ni_types[j+1],'bytes') AND ni_types[j+1] != 'bytes32'),

          /* payload = hex of exactly len bytes; strings converted later */
          substring(
            data_hex,
            1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2 + 64,
            toUInt32(
              reinterpretAsUInt256(
                reverse(
                  unhex(
                    substring(
                      data_hex,
                      1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2,
                      64
                    )
                  )
                )
              )
            ) * 2
          ),

          /* -------- STATIC SCALARS -------- */
          if(
            data_words[j+1] IS NOT NULL,
            multiIf(
              ni_types[j+1] = 'bytes32',
                concat('0x', data_words[j+1]),

              ni_types[j+1] = 'address',
                concat('0x', substring(data_words[j+1], 25, 40)),

              startsWith(ni_types[j+1],'uint')
              OR ni_types[j+1] = 'bool',
                /* bool is stored as a 0/1 uint256 word — same decode path
                   as uint*. Output is '0' or '1' (decimal string). */
                toString(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))),

              startsWith(ni_types[j+1],'int'),
                toString(reinterpretAsInt256(reverse(unhex(data_words[j+1])))),

              NULL
            ),
            NULL
          )
        )
      ),
      range(length(ni_types))
    ) AS raw_values_str,

    -- Human-friendly normalization to STRING:
    -- - Arrays already JSON strings: pass through
    -- - Strings: hex → utf8 (remove NULs)
    -- - Bytes/bytesN: ensure 0x prefix
    arrayMap(j ->
      multiIf(
        endsWith(ni_types[j+1],'[]') AND raw_values_str[j+1] IS NOT NULL,
          raw_values_str[j+1],

        ni_types[j+1] = 'string' AND raw_values_str[j+1] IS NOT NULL,
          replaceRegexpAll(reinterpretAsString(unhex(raw_values_str[j+1])),'\0',''),

        ((ni_types[j+1] = 'bytes') OR (startsWith(ni_types[j+1],'bytes') AND ni_types[j+1] != 'bytes32'))
          AND raw_values_str[j+1] IS NOT NULL,
          concat('0x', raw_values_str[j+1]),

        /* else */
        raw_values_str[j+1]
      ),
      range(length(ni_types))
    ) AS decoded_ni_values,

    -- positions of indexed params (0-based positions into the param list)
    arrayMap(x -> x.3,
      arrayFilter((f,t,i) -> f, arrayZip(a.flags, a.types, range(n_params)))
    ) AS indexed_positions,

    -- stitch back into full order (correct topic index using 1-based indexOf)
    arrayMap(i ->
      if(
        param_flags[i+1],
        /* k1 is 1-based; 0 means not found */
        multiIf(
          indexOf(indexed_positions, i) = 0,
            NULL,
          param_types[i+1] = 'address',
            concat(
              '0x',
              substring(
                replaceAll(arrayElement(raw_topics, indexOf(indexed_positions, i)), '0x',''),
                25, 40
              )
            ),
          startsWith(param_types[i+1],'uint')
          OR param_types[i+1] = 'bool',
            /* Indexed bool: same reinterpret path as uint*. Output '0'/'1'. */
            toString(
              reinterpretAsUInt256(
                reverse(
                  unhex(
                    replaceAll(arrayElement(raw_topics, indexOf(indexed_positions, i)), '0x','')
                  )
                )
              )
            ),
          startsWith(param_types[i+1],'int'),
            toString(
              reinterpretAsInt256(
                reverse(
                  unhex(
                    replaceAll(arrayElement(raw_topics, indexOf(indexed_positions, i)), '0x','')
                  )
                )
              )
            ),
          /* default: bytes32/topic hash as 0x + 64 hex chars */
          concat(
            '0x',
            substring(
              replaceAll(arrayElement(raw_topics, indexOf(indexed_positions, i)), '0x',''),
              1, 64
            )
          )
        ),
        /* non-indexed: pick correct decoded value */
        decoded_ni_values[indexOf(ni_positions, i)]
      ),
      range(n_params)
    ) AS param_values,

    -- final JSON or map (all values are full strings; arrays are JSON strings)
    
      mapFromArrays(param_names, param_values) AS decoded_params
    

  FROM logs_with_abi AS l
  ANY LEFT JOIN abi AS a
    ON replaceAll(l.topic0,'0x','') = a.topic0_sig
   AND l.abi_join_address = a.abi_contract_address
)

SELECT
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  contract_address,
  event_name,
  decoded_params
FROM process
