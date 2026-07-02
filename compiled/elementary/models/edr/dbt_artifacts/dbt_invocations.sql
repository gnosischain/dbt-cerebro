

select * from (
            select
            
                
        cast('this_is_just_a_long_dummy_string' as String) as invocation_id

,
                
        cast('this_is_just_a_long_dummy_string' as String) as job_id

,
                
        cast('this_is_just_a_long_dummy_string' as String) as job_name

,
                
        cast('this_is_just_a_long_dummy_string' as String) as job_run_id

,
                
        cast('dummy_string' as String) as run_started_at

,
                
        cast('dummy_string' as String) as run_completed_at

,
                
        cast('dummy_string' as String) as generated_at

,
                cast('2091-02-17' as DateTime) as created_at

,
                
        cast('dummy_string' as String) as command

,
                
        cast('dummy_string' as String) as dbt_version

,
                
        cast('dummy_string' as String) as elementary_version

,
                
        cast (True as boolean) as full_refresh

,
                
        cast('this_is_just_a_long_dummy_string' as String) as invocation_vars

,
                
        cast('this_is_just_a_long_dummy_string' as String) as vars

,
                
        cast('dummy_string' as String) as target_name

,
                
        cast('dummy_string' as String) as target_database

,
                
        cast('dummy_string' as String) as target_schema

,
                
        cast('dummy_string' as String) as target_profile_name

,
                
        cast(123456789 as Int32) as threads

,
                
        cast('this_is_just_a_long_dummy_string' as String) as selected

,
                
        cast('this_is_just_a_long_dummy_string' as String) as yaml_selector

,
                
        cast('dummy_string' as String) as project_id

,
                
        cast('dummy_string' as String) as project_name

,
                
        cast('dummy_string' as String) as env

,
                
        cast('dummy_string' as String) as env_id

,
                
        cast('dummy_string' as String) as cause_category

,
                
        cast('this_is_just_a_long_dummy_string' as String) as cause

,
                
        cast('dummy_string' as String) as pull_request_id

,
                
        cast('dummy_string' as String) as git_sha

,
                
        cast('dummy_string' as String) as orchestrator

,
                
        cast('dummy_string' as String) as dbt_user

,
                
        cast('dummy_string' as String) as job_url

,
                
        cast('dummy_string' as String) as job_run_url

,
                
        cast('dummy_string' as String) as account_id

,
                
        cast('this_is_just_a_long_dummy_string' as String) as target_adapter_specific_fields


        ) as empty_table
        where 1 = 0