WITH hardware_config AS (
    SELECT * FROM (
        VALUES 
            (4, 'Intel i5-1135G7', '4/8', 'x86/x64', '16 GB', '2 TB SSD', 'Onboard', '65 Watt', 'Integrated', 'Ubuntu 20.04'),
            (5, 'Intel i5-10400', '6/12', 'x86/x64', '64 GB', '2TB SSD', 'Onboard', '650 Watt', 'Custom', 'Ubuntu 21'),
            (6, 'AMD 3970X', '32/64', 'x86/x64', '256 GB', '2TB SSD', 'AM 6970', '1000 Watt', 'Custom', 'Ubuntu 20.04')
    ) AS t(type, cpu, cores_threads, architecture, ram, storage, gpu, psu, "case", os)
)

SELECT * FROM hardware_config