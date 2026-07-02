


    select * from (
            select
            
                
        cast('dummy_string' as String) as source_freshness_execution_id

,
                
        cast('dummy_string' as String) as unique_id

,
                
        cast('dummy_string' as String) as max_loaded_at

,
                
        cast('dummy_string' as String) as snapshotted_at

,
                
        cast('dummy_string' as String) as generated_at

,
                cast('2091-02-17' as DateTime) as created_at

,
                
        cast(123456789.99 as Float32) as max_loaded_at_time_ago_in_s

,
                
        cast('dummy_string' as String) as status

,
                
        cast('dummy_string' as String) as error

,
                
        cast('dummy_string' as String) as compile_started_at

,
                
        cast('dummy_string' as String) as compile_completed_at

,
                
        cast('dummy_string' as String) as execute_started_at

,
                
        cast('dummy_string' as String) as execute_completed_at

,
                
        cast('dummy_string' as String) as invocation_id

,
                
        cast('dummy_string' as String) as warn_after

,
                
        cast('dummy_string' as String) as error_after

,
                
        cast('this_is_just_a_long_dummy_string' as String) as filter


        ) as empty_table
        where 1 = 0
