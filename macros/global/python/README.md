# Agent Snowflake Client for Cursor

This module provides a safe interface for Cursor's agent mode to query your Snowflake instance. It's designed to be secure and only allows SELECT statements.

## Features

- **Security First**: Only SELECT statements are allowed
- **Environment Variable Based**: Uses your existing Snowflake credentials from environment variables
- **JSON Output**: Returns results in a format that's easy for agents to consume
- **Error Handling**: Comprehensive error handling and logging
- **Table Information**: Helper methods to get table structure and sample data
- **DBT Naming Convention**: Automatically parses DBT model names (e.g., `silver__aave_borrows` → `SILVER.aave_borrows`)

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Variables**: Ensure these are set in your environment:
   - `ACCOUNT`: Your Snowflake account identifier
   - `USER`: Your Snowflake username
   - `PASSWORD`: Your Snowflake password
   - `ROLE`: Your Snowflake role
   - `WAREHOUSE`: Your Snowflake warehouse
   - `DATABASE`: Your Snowflake database
   - `REGION`: Your Snowflake region

## Usage

### Basic Query Execution

```python
from agent_snowflake_client import AgentSnowflakeClient

client = AgentSnowflakeClient()

# Execute a simple query
result = client.execute_query("SELECT CURRENT_TIMESTAMP() as current_time")
print(result)
```

### DBT Naming Convention

The client automatically handles DBT naming conventions:

```python
# These are equivalent:
client.get_sample_data("silver__dim_variables")  # DBT format
client.get_sample_data("dim_variables", schema="SILVER")  # Explicit format

# Both translate to: SELECT * FROM SILVER.dim_variables LIMIT 10
```

Supported patterns:
- `silver__table_name` → `SILVER.table_name`
- `gold__table_name` → `GOLD.table_name`
- `bronze__table_name` → `BRONZE.table_name`

### Get Table Information

```python
# Get table structure
table_info = client.get_table_info("YOUR_TABLE_NAME", schema="SILVER")
print(table_info)
```

### Get Sample Data

```python
# Get sample data from a table
sample_data = client.get_sample_data("YOUR_TABLE_NAME", limit=10, schema="SILVER")
print(sample_data)
```

### Command Line Usage

```bash
python agent_snowflake_client.py "SELECT * FROM your_table LIMIT 5"
```

## Output Format

The client returns results in this JSON format:

```json
{
  "success": true,
  "columns": ["column1", "column2", "column3"],
  "rows": [
    {"column1": "value1", "column2": "value2", "column3": "value3"},
    {"column1": "value4", "column2": "value5", "column3": "value6"}
  ],
  "row_count": 2,
  "query": "SELECT * FROM table LIMIT 2"
}
```

## Security Features

- **SELECT Only**: Only SELECT statements are allowed
- **Keyword Blocking**: Dangerous keywords like DELETE, DROP, UPDATE, INSERT, etc. are blocked
- **Environment Variables**: Credentials are stored securely in environment variables
- **Error Handling**: Comprehensive error handling prevents information leakage

## Integration with Cursor Agent Mode

To use this with Cursor's agent mode, you can:

1. **Direct Python Execution**: Have the agent run Python scripts that use this client
2. **Command Line Interface**: Use the command-line interface for simple queries
3. **Custom Functions**: Create wrapper functions for specific use cases

### Example Agent Usage

```python
# Agent can use this to explore your data
def explore_table(table_name, schema="SILVER"):
    client = AgentSnowflakeClient()
    
    # Get table structure
    structure = client.get_table_info(table_name, schema)
    
    # Get sample data
    sample = client.get_sample_data(table_name, limit=5, schema=schema)
    
    return {
        "structure": structure,
        "sample_data": sample
    }
```

## Testing

Run the test script to verify everything is working:

```bash
python test_agent_query.py
```

This will test:
- Basic query execution
- Security features (blocking dangerous queries)
- Error handling

## DBT Integration

The system also includes a DBT macro (`agent_snowflake_query.sql`) that can be used within your DBT models for testing and validation purposes.

## Troubleshooting

1. **Connection Issues**: Verify all environment variables are set correctly
2. **Permission Errors**: Ensure your Snowflake role has appropriate permissions
3. **Query Errors**: Check that your queries are valid Snowflake SQL
4. **Security Errors**: Ensure you're only using SELECT statements

## Best Practices

1. **Use Specific Queries**: Instead of `SELECT *`, specify the columns you need
2. **Add LIMIT Clauses**: Always add LIMIT clauses to prevent large result sets
3. **Test First**: Use the test script to verify your setup
4. **Monitor Usage**: Keep track of query performance and resource usage
5. **Error Handling**: Always handle potential errors in your agent code
