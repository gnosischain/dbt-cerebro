

select * from (
            select
            
                
        cast('this_is_just_a_long_dummy_string' as String) as model_execution_id

,
                
        cast('this_is_just_a_long_dummy_string' as String) as unique_id

,
                
        cast('dummy_string' as String) as invocation_id

,
                
        cast('dummy_string' as String) as generated_at

,
                cast('2091-02-17' as DateTime) as created_at

,
                
        cast('this_is_just_a_long_dummy_string' as String) as name

,
                
        cast('this_is_just_a_long_dummy_string' as String) as message

,
                
        cast('dummy_string' as String) as status

,
                
        cast('dummy_string' as String) as resource_type

,
                
        cast(123456789.99 as Float32) as execution_time

,
                
        cast('dummy_string' as String) as execute_started_at

,
                
        cast('dummy_string' as String) as execute_completed_at

,
                
        cast('dummy_string' as String) as compile_started_at

,
                
        cast('dummy_string' as String) as compile_completed_at

,
                
        cast(31474836478 as bigint) as rows_affected

,
                
        cast (True as boolean) as full_refresh

,
                
        cast('this_is_just_a_long_dummy_string' as String) as compiled_code

,
                
        cast(31474836478 as bigint) as failures

,
                
        cast('dummy_string' as String) as query_id

,
                
        cast('dummy_string' as String) as thread_id

,
                
        cast('dummy_string' as String) as materialization

,
                
        cast('dummy_string' as String) as adapter_response

,
                
        cast('dummy_string' as String) as group_name


        ) as empty_table
        where 1 = 0