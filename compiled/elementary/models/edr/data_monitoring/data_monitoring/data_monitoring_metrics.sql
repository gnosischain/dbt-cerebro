


    
    
        
    
    select * from (
            select
            
                
        cast('dummy_string' as String) as id

,
                
        cast('dummy_string' as Nullable(String)) as full_table_name

,
                
        cast('dummy_string' as Nullable(String)) as column_name

,
                
        cast('dummy_string' as Nullable(String)) as metric_name

,
                
        cast('dummy_string' as Nullable(String)) as metric_type

,
                cast(123456789.99 as Nullable(Float32)) as metric_value

,
                
        cast('dummy_string' as Nullable(String)) as source_value

,
                cast('2091-02-17' as DateTime) as bucket_start

,
                cast('2091-02-17' as DateTime) as bucket_end

,
                cast(123456789 as Nullable(Int32)) as bucket_duration_hours

,
                cast('2091-02-17' as Nullable(DateTime)) as updated_at

,
                
        cast('dummy_string' as Nullable(String)) as dimension

,
                
        cast('dummy_string' as Nullable(String)) as dimension_value

,
                
        cast('dummy_string' as String) as metric_properties

,
                cast('2091-02-17' as Nullable(DateTime)) as created_at


        ) as empty_table
        where 1 = 0
