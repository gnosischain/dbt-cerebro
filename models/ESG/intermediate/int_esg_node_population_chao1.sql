{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='observation_date',
        partition_by='toStartOfMonth(observation_date)',
        order_by='observation_date',
        tags=['production','esg','nodes'],
        settings={
            'allow_nullable_key': 1
        }
    )
}}

WITH peer_connection_analysis AS (
    SELECT
        toDate(visit_ended_at) AS observation_date,
        peer_id,
        crawl_id,
        
        -- Categorize connection attempts
        CASE 
            WHEN empty(dial_errors) = 1 AND crawl_error IS NULL THEN 'successful'
            WHEN empty(dial_errors) = 0 OR crawl_error IS NOT NULL THEN 'failed'
            ELSE 'unknown'
        END AS connection_status,
        
        -- More granular failure analysis
        CASE 
            WHEN empty(dial_errors) = 1 AND crawl_error IS NULL THEN 'successful'
            WHEN crawl_error LIKE '%timeout%' THEN 'timeout'
            WHEN crawl_error LIKE '%refused%' OR crawl_error LIKE '%connection refused%' THEN 'refused' 
            WHEN crawl_error LIKE '%unreachable%' THEN 'unreachable'
            WHEN crawl_error LIKE '%protocol%' THEN 'protocol_mismatch'
            WHEN NOT empty(dial_errors) THEN 'dial_error'
            ELSE 'other_failure'
        END AS failure_type
        
    FROM {{ ref('int_p2p_discv5_peers') }}
    WHERE
        toStartOfDay(visit_ended_at) < today()
        {{ apply_monthly_incremental_filter('visit_ended_at','observation_date','true') }}
),

-- Chao-1 for successful connections only
successful_chao1 AS (
    SELECT
        observation_date,
        peer_id,
        COUNT(DISTINCT crawl_id) AS times_observed
    FROM peer_connection_analysis
    WHERE connection_status = 'successful'
    {% if is_incremental() %}
        AND observation_date > (SELECT MAX(observation_date) FROM {{ this }})
    {% endif %}
    GROUP BY observation_date, peer_id
),

successful_stats AS (
    SELECT
        observation_date,
        COUNT(DISTINCT peer_id) AS s_obs_successful,
        SUM(CASE WHEN times_observed = 1 THEN 1 ELSE 0 END) AS f1_successful,
        SUM(CASE WHEN times_observed = 2 THEN 1 ELSE 0 END) AS f2_successful
    FROM successful_chao1
    GROUP BY observation_date
),

-- Chao-1 for ALL connection attempts (successful + failed)
all_attempts_chao1 AS (
    SELECT
        observation_date,
        peer_id,
        COUNT(DISTINCT crawl_id) AS times_observed
    FROM peer_connection_analysis
    {% if is_incremental() %}
        WHERE observation_date > (SELECT MAX(observation_date) FROM {{ this }})
    {% endif %}
    GROUP BY observation_date, peer_id
),

all_attempts_stats AS (
    SELECT
        observation_date,
        COUNT(DISTINCT peer_id) AS s_obs_all,
        SUM(CASE WHEN times_observed = 1 THEN 1 ELSE 0 END) AS f1_all,
        SUM(CASE WHEN times_observed = 2 THEN 1 ELSE 0 END) AS f2_all
    FROM all_attempts_chao1
    GROUP BY observation_date
),

-- Additional peers known from failed connections
peer_status_summary AS (
    SELECT
        observation_date,
        peer_id,
        MAX(CASE WHEN connection_status = 'successful' THEN 1 ELSE 0 END) AS had_success,
        MAX(CASE WHEN connection_status = 'failed' THEN 1 ELSE 0 END) AS had_failure
    FROM peer_connection_analysis
    {% if is_incremental() %}
        WHERE observation_date > (SELECT MAX(observation_date) FROM {{ this }})
    {% endif %}
    GROUP BY observation_date, peer_id
),

failed_only_peers AS (
    SELECT
        observation_date,
        COUNT(DISTINCT peer_id) AS peers_with_only_failures
    FROM peer_status_summary
    WHERE had_failure = 1 AND had_success = 0
    GROUP BY observation_date
),

-- Connection success rates by failure type
failure_analysis AS (
    SELECT
        observation_date,
        failure_type,
        COUNT(DISTINCT peer_id) AS peer_count,
        COUNT(*) AS attempt_count,
        
        -- Estimate reachability probability based on failure type
        CASE failure_type
            WHEN 'timeout' THEN 0.3      -- Sometimes reachable
            WHEN 'refused' THEN 0.1      -- Rarely reachable (firewall/NAT)
            WHEN 'unreachable' THEN 0.05 -- Very rarely reachable
            WHEN 'protocol_mismatch' THEN 0.8  -- Likely reachable with right protocol
            WHEN 'dial_error' THEN 0.2   -- Sometimes reachable
            ELSE 0.1
        END AS estimated_reachability_prob
        
    FROM peer_connection_analysis
    WHERE connection_status = 'failed'
    {% if is_incremental() %}
        AND observation_date > (SELECT MAX(observation_date) FROM {{ this }})
    {% endif %}
    GROUP BY observation_date, failure_type
),

-- Calculate enhanced estimates
enhanced_calculations AS (
    SELECT
        COALESCE(s.observation_date, a.observation_date) AS observation_date,
        
        -- Successful connection metrics
        COALESCE(s.s_obs_successful, 0) AS observed_successful_nodes,
        CASE
            WHEN COALESCE(s.f2_successful, 0) > 0 THEN 
                COALESCE(s.s_obs_successful, 0) + toFloat64(s.f1_successful * (s.f1_successful - 1)) / (2.0 * toFloat64(s.f2_successful + 1))
            WHEN COALESCE(s.f1_successful, 0) > 0 THEN 
                COALESCE(s.s_obs_successful, 0) + toFloat64(s.f1_successful * (s.f1_successful - 1)) / 2.0
            ELSE 
                toFloat64(COALESCE(s.s_obs_successful, 0))
        END AS chao1_successful,
        
        -- All attempt metrics  
        COALESCE(a.s_obs_all, 0) AS observed_total_peers,
        CASE
            WHEN COALESCE(a.f2_all, 0) > 0 THEN 
                COALESCE(a.s_obs_all, 0) + toFloat64(a.f1_all * (a.f1_all - 1)) / (2.0 * toFloat64(a.f2_all + 1))
            WHEN COALESCE(a.f1_all, 0) > 0 THEN 
                COALESCE(a.s_obs_all, 0) + toFloat64(a.f1_all * (a.f1_all - 1)) / 2.0
            ELSE 
                toFloat64(COALESCE(a.s_obs_all, 0))
        END AS chao1_all_attempts,
        
        -- Failed connection insights
        COALESCE(f.peers_with_only_failures, 0) AS failed_only_peers,
        
        COALESCE(s.f1_successful, 0) AS f1_successful,
        COALESCE(s.f2_successful, 0) AS f2_successful,
        COALESCE(a.f1_all, 0) AS f1_all,
        COALESCE(a.f2_all, 0) AS f2_all
        
    FROM successful_stats s
    FULL OUTER JOIN all_attempts_stats a ON s.observation_date = a.observation_date
    LEFT JOIN failed_only_peers f ON COALESCE(s.observation_date, a.observation_date) = f.observation_date
),

-- Separate CTE for failure analysis aggregation
failure_reachability AS (
    SELECT
        observation_date,
        SUM(toFloat64(peer_count) * estimated_reachability_prob) AS estimated_reachable_from_failures
    FROM failure_analysis
    GROUP BY observation_date
),

-- Combine all estimates
combined_estimates AS (
    SELECT
        e.*,
        COALESCE(fr.estimated_reachable_from_failures, 0.0) AS estimated_reachable_from_failures
    FROM enhanced_calculations e
    LEFT JOIN failure_reachability fr ON e.observation_date = fr.observation_date
),

final_estimates AS (
    SELECT
        observation_date,
        observed_successful_nodes,
        observed_total_peers,
        failed_only_peers,
        
        -- Different estimation approaches
        toUInt64(round(chao1_successful, 0)) AS chao1_successful_only,
        toUInt64(round(chao1_all_attempts, 0)) AS chao1_all_discovered,
        toUInt64(round(estimated_reachable_from_failures, 0)) AS estimated_additional_reachable,
        
        -- Enhanced total estimate combining multiple signals
        toUInt64(round(
            chao1_successful +  -- Hidden successful nodes
            estimated_reachable_from_failures  -- Additional reachable from failures
        , 0)) AS enhanced_total_reachable,
        
        -- Network size estimate (includes unreachable nodes)
        toUInt64(round(chao1_all_attempts, 0)) AS estimated_network_size,
        
        -- Success rates
        CASE WHEN observed_total_peers > 0 THEN 
            round(100.0 * observed_successful_nodes / observed_total_peers, 2)
        ELSE 0 END AS connection_success_rate_pct,
        
        -- Coverage estimates
        CASE WHEN chao1_all_attempts > 0 THEN
            round(100.0 * observed_total_peers / chao1_all_attempts, 2)
        ELSE 100 END AS network_discovery_coverage_pct,
        
        CASE WHEN enhanced_total_reachable > 0 THEN
            round(100.0 * observed_successful_nodes / enhanced_total_reachable, 2) 
        ELSE 100 END AS reachable_discovery_coverage_pct,
        
        -- Diagnostic info
        f1_successful, f2_successful, f1_all, f2_all
        
    FROM combined_estimates
)

SELECT
    observation_date,
    
    -- Core metrics
    observed_successful_nodes,
    observed_total_peers, 
    failed_only_peers,
    
    -- Population estimates
    chao1_successful_only,
    enhanced_total_reachable,
    estimated_network_size,
    estimated_additional_reachable,
    
    -- Success and coverage rates
    connection_success_rate_pct,
    network_discovery_coverage_pct,
    reachable_discovery_coverage_pct,
    
    -- Hidden node estimates
    chao1_successful_only - observed_successful_nodes AS hidden_successful_nodes,
    enhanced_total_reachable - observed_successful_nodes AS hidden_reachable_nodes,
    estimated_network_size - observed_total_peers AS hidden_total_nodes,
    
    -- Percentages
    CASE WHEN chao1_successful_only > 0 THEN
        round(100.0 * (chao1_successful_only - observed_successful_nodes) / chao1_successful_only, 2)
    ELSE 0 END AS hidden_successful_pct,
    
    CASE WHEN estimated_network_size > 0 THEN
        round(100.0 * (estimated_network_size - observed_total_peers) / estimated_network_size, 2) 
    ELSE 0 END AS hidden_total_pct,
    
    -- Diagnostic information
    f1_successful AS successful_singletons,
    f2_successful AS successful_doubletons,
    f1_all AS all_singletons,
    f2_all AS all_doubletons,
    
    now() AS calculated_at

FROM final_estimates
ORDER BY observation_date DESC