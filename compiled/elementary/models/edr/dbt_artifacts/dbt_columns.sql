

select * from (
            select
            
                
        cast('dummy_string' as String) as unique_id

,
                
        cast('dummy_string' as String) as parent_unique_id

,
                
        cast('dummy_string' as String) as name

,
                
        cast('dummy_string' as String) as data_type

,
                
        cast('this_is_just_a_long_dummy_string' as String) as tags

,
                
        cast('this_is_just_a_long_dummy_string' as String) as meta

,
                
        cast('dummy_string' as String) as database_name

,
                
        cast('dummy_string' as String) as schema_name

,
                
        cast('dummy_string' as String) as table_name

,
                
        cast('this_is_just_a_long_dummy_string' as String) as description

,
                
        cast('dummy_string' as String) as resource_type

,
                
        cast('dummy_string' as String) as generated_at

,
                
        cast('dummy_string' as String) as metadata_hash


        ) as empty_table
        where 1 = 0