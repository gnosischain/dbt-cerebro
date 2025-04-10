import os
import yaml
import re
from pathlib import Path
import sqlparse

# Regex to detect columns from inline comments
COMMENT_DESCRIPTION_PATTERN = re.compile(
    r"--\s*@column\s+(?P<col_name>\w+)\s*:\s*(?P<description>.*)$", re.IGNORECASE
)
# Regex to detect columns from Jinja comments
JINJA_COMMENT_DESCRIPTION_PATTERN = re.compile(
    r"{#\s*@column\s+(?P<col_name>\w+)\s*:\s*(?P<description>.*?)#}", re.IGNORECASE | re.DOTALL
)
# Regex to detect dbt references, e.g. ref('some_model')
REF_PATTERN = re.compile(r"ref\(['\"]([^'\"]+)['\"]\)")

def extract_column_comments(sql_file_path):
    """
    Extract column descriptions from specially formatted comments in the SQL file.
    Looks for lines like:
        -- @column col_name: This is the description for col_name
    or in Jinja comments:
        {# @column col_name: Description #}
    Returns a dict: {col_name: description}.
    """
    with open(sql_file_path, 'r') as f:
        content = f.read()

    # Combine normal comment-based matches and Jinja comment-based matches
    matches = COMMENT_DESCRIPTION_PATTERN.findall(content) + JINJA_COMMENT_DESCRIPTION_PATTERN.findall(content)

    column_desc_map = {}
    for match in matches:
        col_name, description = match[0], match[1]
        column_desc_map[col_name.strip()] = description.strip()

    return column_desc_map

def extract_references(sql_file_path):
    """
    Extract references from the file by looking for ref('model_name').
    """
    with open(sql_file_path, 'r') as f:
        content = f.read()
    return REF_PATTERN.findall(content)

def split_on_top_level_comma(expr):
    """
    Split a SQL expression on top-level commas (commas not inside parentheses).
    Example:
      "col1, sum(col2), case when x then y end as col3"
    returns:
      ["col1", "sum(col2)", "case when x then y end as col3"]
    """
    result = []
    current = []
    depth = 0
    for char in expr:
        if char == '(':
            depth += 1
            current.append(char)
        elif char == ')':
            depth -= 1
            current.append(char)
        elif char == ',' and depth == 0:
            result.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        result.append("".join(current).strip())
    return result

def extract_columns_from_sql(sql_file_path):
    """
    Parse SQL to find columns in SELECT statements. It looks for patterns from
    SELECT ... FROM, then splits columns on top-level commas.
    """
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()

    parsed_statements = sqlparse.parse(sql_content)
    columns_set = set()

    for statement in parsed_statements:
        # Find tokens that are SELECT. For each, parse the block from SELECT to FROM
        for token in statement.tokens:
            if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
                # Attempt to match from SELECT ... FROM
                select_span = re.search(r'SELECT\s+(.*?)\s+FROM', str(statement), re.IGNORECASE | re.DOTALL)
                if select_span:
                    select_block = select_span.group(1)
                    columns_raw = split_on_top_level_comma(select_block)
                    for col_expr in columns_raw:
                        col_expr = col_expr.strip()
                        # If there's an alias, use that as the name; otherwise grab last piece
                        as_match = re.search(r'\s+AS\s+([`"\'\[\]\w]+)$', col_expr, re.IGNORECASE)
                        if as_match:
                            col = as_match.group(1)
                        else:
                            # No 'AS' -> take the last chunk after splitting by '.' or space
                            parts = re.split(r'[\s\.]+', col_expr)
                            col = parts[-1]
                        col = col.strip(' "\'`[]()')
                        if col and not col.startswith('('):
                            columns_set.add(col)

    return sorted(columns_set)

def get_metadata_from_path(path):
    """
    Extract metadata from path. The first folder after 'models' is considered the sector.
    Any subsequent folders become additional tags.
    Example path:
      models/execution/some_subfolder/my_model.sql
    => sector='execution', tags=['execution','some_subfolder']
    """
    parts = path.parts
    try:
        models_index = parts.index('models')
    except ValueError:
        # Fallback if 'models' not found
        models_index = 0

    # Next folder after 'models' is the sector
    sector_idx = models_index + 1
    sector = parts[sector_idx] if sector_idx < len(parts) else None

    # Additional tags from deeper subfolders
    tags = []
    if sector:
        tags.append(sector)

    # Everything after sector is an additional tag (except the .sql file itself)
    for part in parts[sector_idx+1:]:
        if not part.endswith('.sql'):
            tags.append(part)

    return {
        'sector': sector,
        'tags': tags
    }

def create_model_entry(file_path):
    """
    Create model entry with metadata, config, and columns for schema.yml.
    """
    model_name = os.path.splitext(os.path.basename(file_path))[0]
    metadata = get_metadata_from_path(Path(file_path))

    # Extract columns from the SQL
    columns_inferred = extract_columns_from_sql(file_path)
    # Extract user-defined descriptions from comments
    column_desc_map = extract_column_comments(file_path)

    # Build up columns array for the YAML
    columns_list = []
    for col in columns_inferred:
        col_entry = {
            'name': col,
            'description': column_desc_map.get(col, f"TODO: Add description for {col}"),
            'data_tests': [
                'not_null'
            ]
        }
        columns_list.append(col_entry)

    # Extract references if you want to store them in meta
    refs = extract_references(file_path)

    model_entry = {
        'name': model_name,
        'description': f'TODO: Add description for {model_name}',
        'meta': {
            'sector': metadata['sector'],
            'refs': refs,
            'contributors': 'TODO: Add contributors'
        },
        'config': {
            'tags': metadata['tags']
        },
        'columns': columns_list
    }

    # Example: add a unique-combination-of-columns test for the first 2 columns
    if len(columns_inferred) >= 2:
        model_entry['data_tests'] = [
            {
                'dbt_utils.unique_combination_of_columns': {
                    'combination_of_columns': columns_inferred[:2]
                }
            }
        ]

    return model_entry

def create_schema_yaml(directory_path, models):
    """
    Generate schema.yml in the given directory with the provided list of models.
    """
    schema_content = {
        'version': 2,
        'models': models
    }

    schema_path = os.path.join(directory_path, 'schema.yml')
    with open(schema_path, 'w') as f:
        yaml.dump(schema_content, f, sort_keys=False, default_flow_style=False, allow_unicode=True)

def process_directory(models_path):
    """
    Recursively walk through the 'models_path', and in each directory containing
    .sql files, generate a schema.yml file listing those models.
    """
    for root, dirs, files in os.walk(models_path):
        sql_files = [f for f in files if f.endswith('.sql')]
        if not sql_files:
            continue

        models = []
        for sql_file in sql_files:
            file_path = os.path.join(root, sql_file)
            model_entry = create_model_entry(file_path)
            models.append(model_entry)

        if models:
            create_schema_yaml(root, models)
            print(f"Created schema.yml in {root}")

def main():
    current_dir = os.getcwd()
    models_path = os.path.join(current_dir, 'models')

    if not os.path.exists(models_path):
        print(f"Error: {models_path} directory not found")
        return

    process_directory(models_path)
    print("Schema files generation completed!")

if __name__ == "__main__":
    main()
