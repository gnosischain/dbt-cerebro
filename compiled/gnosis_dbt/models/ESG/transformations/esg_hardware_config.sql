WITH hardware_config AS (
    SELECT
        type
        ,cpu
        ,cores_threads
        ,architecture
        ,ram
        ,storage
        ,gpu
        ,psu
        ,"case"
        ,os
    FROM (
        SELECT
            arrayJoin([4, 5, 6]) AS type,
            arrayJoin(['Intel i5-1135G7', 'Intel i5-10400', 'AMD 3970X']) AS cpu,
            arrayJoin(['4/8', '6/12', '32/64']) AS cores_threads,
            arrayJoin(['x86/x64', 'x86/x64', 'x86/x64']) AS architecture,
            arrayJoin(['16 GB', '64 GB', '256 GB']) AS ram,
            arrayJoin(['2 TB SSD', '2TB SSD', '2TB SSD']) AS storage,
            arrayJoin(['Onboard', 'Onboard', 'AM 6970']) AS gpu,
            arrayJoin(['65 Watt', '650 Watt', '1000 Watt']) AS psu,
            arrayJoin(['Integrated', 'Custom', 'Custom']) AS "case",
            arrayJoin(['Ubuntu 20.04', 'Ubuntu 21', 'Ubuntu 20.04']) AS os
    )
)

SELECT * FROM hardware_config