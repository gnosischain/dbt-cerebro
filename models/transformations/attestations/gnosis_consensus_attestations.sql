-- models/gnosis/gnosis_enr_ranked_records.sql
{{ 
    config(
            materialized='table',
			engine='AggregatingMergeTree()',
			partition_by='toYYYYMMDD(f_inclusion_slot)',
			order_by='(f_inclusion_slot, inc_dist_cohort)',
			primary_key='(f_inclusion_slot, inc_dist_cohort)'
        ) 
}}


WITH

total_solts AS (
    SELECT
        f_slot
    FROM
        {{ source('gnosis_chaind', 't_proposer_duties') }}
),

proposed_solts AS (
    SELECT
        f_slot
    FROM
        {{ source('gnosis_chaind', 't_blocks') }}
),

inclusion_distance AS (
	SELECT
		a.f_inclusion_slot
		,a.f_slot   
		,a.f_inclusion_index
		,COUNT(p.f_slot) AS inc_dist_cohort
	FROM
		{{ source('gnosis_chaind', 't_attestations') }} a
	LEFT JOIN
		proposed_solts p
		ON
		p.f_slot>a.f_slot AND p.f_slot<=a.f_inclusion_slot
	GROUP BY 1, 2, 3
)

SELECT
    f_inclusion_slot
	--DATE_TRUNC('hour', compute_timestamp_at_slot(f_inclusion_slot)) AS timestamp
	,inc_dist_cohort
	,COUNT(*) AS cnt
FROM
	inclusion_distance 
GROUP BY 1, 2

