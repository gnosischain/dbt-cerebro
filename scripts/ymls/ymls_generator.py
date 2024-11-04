import os
import yaml
import re
from pathlib import Path
import sqlparse

def extract_columns_from_sql(sql_file_path):
    """Extract column names and attempt to infer descriptions from SQL file."""
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()
    
    # Parse SQL content
    parsed = sqlparse.parse(sql_content)[0]
    
    # Look for SELECT statements and CTEs
    columns = set()
    for token in parsed.tokens:
        if isinstance(token, sqlparse.sql.Token):
            # Extract column names from SELECT statements
            if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
                select_columns = re.findall(r'SELECT\s+(.+?)\s+FROM', sql_content, re.IGNORECASE | re.DOTALL)
                if select_columns:
                    cols = select_columns[0].split(',')
                    for col in cols:
                        # Extract column name, handling aliases
                        col = col.strip()
                        if ' AS ' in col.upper():
                            col = col.split(' AS ')[-1]
                        # Remove any table qualifiers
                        col = col.split('.')[-1]
                        # Clean up any remaining whitespace or quotes
                        col = col.strip('` "\'\n\t')
                        if col and not col.startswith('('):
                            columns.add(col)
    
    # Create column entries
    column_entries = []
    for col in sorted(columns):
        entry = {
            'name': col,
            'description': f"TODO: Add description for {col}",
            'data_tests': [
                'not_null',
                'unique'
            ]
        }
        column_entries.append(entry)
    
    return column_entries

def get_metadata_from_path(path):
    """Extract metadata from the path structure."""
    parts = path.parts
    
    # Get the first part after 'models' as the blockchain
    blockchain_idx = parts.index('models') + 1
    blockchain = parts[blockchain_idx] if blockchain_idx < len(parts) else None
    
    # Get the next part as the sector
    sector_idx = blockchain_idx + 1
    sector = parts[sector_idx] if sector_idx < len(parts) else None
    
    # Build tags from the path components
    tags = []
    if blockchain:
        tags.append(blockchain)
    if sector:
        tags.append(sector)
        
    # Add additional tags based on folder structure
    for part in parts[sector_idx+1:]:
        if part not in ['metrics', 'transformations'] and not part.endswith('.sql'):
            tags.append(part)
    
    return {
        'blockchain': blockchain,
        'sector': sector,
        'tags': tags
    }

def create_model_entry(file_path):
    """Create a complete model entry including metadata, config, and columns."""
    model_name = os.path.splitext(os.path.basename(file_path))[0]
    metadata = get_metadata_from_path(Path(file_path))
    
    # Extract columns from SQL file
    columns = extract_columns_from_sql(file_path)
    
    # Get first two column names for unique combination test
    first_two_columns = [col['name'] for col in columns[:2]]
    
    model_entry = {
        'name': model_name,
        'meta': {
            'blockchain': metadata['blockchain'],
            'sector': metadata['sector'],
            'contributors': 'TODO: Add contributors'
        },
        'config': {
            'tags': metadata['tags']
        },
        'description': f'TODO: Add description for {model_name}',
        'columns': columns
    }
    
    # Add tests if we have at least two columns
    if len(first_two_columns) >= 2:
        model_entry['data_tests'] = [
            {
                'dbt_utils.unique_combination_of_columns': {
                    'combination_of_columns': first_two_columns
                }
            }
        ]
    
    return model_entry

def create_schema_yaml(directory_path, models):
    """Create a schema.yml file with the given models."""
    schema_content = {
        'version': 2,
        'models': models
    }
    
    schema_path = os.path.join(directory_path, 'schema.yml')
    with open(schema_path, 'w') as f:
        yaml.dump(schema_content, f, sort_keys=False, default_flow_style=False, allow_unicode=True)

def process_directory(models_path):
    """Process directory recursively and create schema.yml files."""
    for root, dirs, files in os.walk(models_path):
        # Skip if no SQL files in directory
        sql_files = [f for f in files if f.endswith('.sql')]
        if not sql_files:
            continue
        
        # Create model entries for each SQL file
        models = []
        for sql_file in sql_files:
            file_path = os.path.join(root, sql_file)
            model_entry = create_model_entry(file_path)
            models.append(model_entry)
        
        # Create schema.yml if we have models
        if models:
            create_schema_yaml(root, models)
            print(f"Created schema.yml in {root}")

def main():
    # Find the models directory
    current_dir = os.getcwd()
    models_path = os.path.join(current_dir, 'models')
    
    if not os.path.exists(models_path):
        print(f"Error: {models_path} directory not found")
        return
    
    process_directory(models_path)
    print("Schema files generation completed!")

if __name__ == "__main__":
    main()