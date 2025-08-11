{{ 
    config(
            materialized='table'
        ) 
}}


WITH

consensus_power AS (
    SELECT
        type
        ,client
        ,mean
    FROM
        {{ ref('esg_consensus_power') }}
),

execution_power AS (
    SELECT
        type
        ,client
        ,mean
    FROM
        {{ ref('esg_execution_power') }}
),

idle_electric_power AS (
    SELECT
        type
        ,mean
    FROM
        {{ ref('esg_idle_electric_power') }}
),

node_distribution AS (
    SELECT
        type
        ,distribution
    FROM
        {{ ref('esg_node_distribution') }}
),

node_config_power AS (
    SELECT
        t1.type
        ,t1.client AS consensus_client
        ,t2.client AS execution_client
        ,t1.mean + t2.mean + t3.mean AS mean
    FROM
        consensus_power t1
    INNER JOIN
        execution_power t2
        ON 
        t2.type = t1.type
    INNER JOIN
        idle_electric_power t3
        ON 
        t3.type = t1.type

),

best_guess_per_client AS (
    SELECT
        t1.consensus_client
        ,t1.execution_client
        ,AVG(t1.mean * t2.distribution) AS mean
    FROM
        node_config_power t1
    INNER JOIN
        node_distribution t2
        ON 
        t2.type = t1.type
    GROUP BY
        t1.consensus_client
        ,t1.execution_client
),

configuration_distribution AS (
    SELECT 
        execution_client
        ,consensus_client
        ,frac
    FROM (
        SELECT
            arrayJoin(['Erigon', 'Erigon', 'Erigon', 'Erigon', 'Nethermind', 'Nethermind', 'Nethermind', 'Nethermind']) AS execution_client,
            arrayJoin(['Lighthouse', 'Teku', 'Lodestar', 'Nimbus', 'Lighthouse', 'Teku', 'Lodestar', 'Nimbus']) AS consensus_client,
            arrayJoin([0.340, 0.114, 0.044, 0.002, 0.340, 0.114, 0.044, 0.002]) AS frac
    )
),

power_best_guess AS (
    SELECT 
        SUM(t1.mean * t2.frac) AS mean
    FROM 
        best_guess_per_client t1
    INNER JOIN
        configuration_distribution t2
        ON
        t2.execution_client = t1.execution_client
        AND
        t2.consensus_client = t1.consensus_client
)


SELECT
    t1.date
    ,t1.country
    ,t1.cnt * t2.mean AS power
FROM
    {{ ref('int_p2p_discv5_geo_daily') }} t1
CROSS JOIN 
    power_best_guess t2