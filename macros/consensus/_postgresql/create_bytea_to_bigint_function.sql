-- models/_postgresql/create_bytea_to_bigint_function.sql

{% macro create_bytea_to_numeric_function() %}

CREATE OR REPLACE FUNCTION bytea_to_numeric(bytes bytea) RETURNS numeric AS $$
DECLARE
    result numeric := 0;
    byte_value int;
    byte_count int;
BEGIN
    IF bytes IS NULL THEN
        RETURN NULL;
    END IF;

    byte_count := octet_length(bytes);
    
    IF byte_count = 0 THEN
        RETURN 0;
    END IF;
    
    FOR i IN 0..byte_count-1 LOOP
        byte_value := get_byte(bytes, i);
        result := result * 256 + byte_value;
    END LOOP;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

{% endmacro %}