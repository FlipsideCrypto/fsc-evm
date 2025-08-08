#!/usr/bin/env python3
"""
Agent Snowflake Client for Cursor
This module provides a safe interface for Cursor's agent mode to query Snowflake.
Only SELECT statements are allowed for security.
"""

import os
import json
import snowflake.connector
from typing import Dict, List, Optional, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AgentSnowflakeClient:
    """Safe Snowflake client for agent mode queries."""
    
    def __init__(self):
        """Initialize the client with environment variables."""
        self.account = os.getenv('ACCOUNT')
        self.user = os.getenv('USER')
        self.password = os.getenv('PASSWORD')
        self.role = os.getenv('ROLE')
        self.warehouse = os.getenv('WAREHOUSE')
        self.database = os.getenv('DATABASE')
        self.region = os.getenv('REGION')
        
        if not all([self.account, self.user, self.password, self.role, self.warehouse, self.database]):
            raise ValueError("Missing required Snowflake environment variables")
    
    def _validate_query(self, query: str) -> bool:
        """Validate that the query is a safe SELECT statement."""
        query_upper = query.strip().upper()
        
        # Only allow SELECT statements
        if not query_upper.startswith('SELECT'):
            raise ValueError("Only SELECT statements are allowed for security")
        
        # Block potentially dangerous keywords
        dangerous_keywords = [
            'DELETE', 'DROP', 'TRUNCATE', 'UPDATE', 'INSERT', 'MERGE',
            'CREATE', 'ALTER', 'GRANT', 'REVOKE', 'EXECUTE', 'CALL'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in query_upper:
                raise ValueError(f"Query contains forbidden keyword: {keyword}")
        
        return True
    
    def get_sample_data(self, table_name: str, limit: int = 10, schema: str = None, database: str = None) -> Dict[str, Any]:
        """
        Get sample data from a table.
        
        Args:
            table_name: Name of the table (can be DBT format like 'silver__aave_borrows')
            limit: Number of rows to return (default 10)
            schema: Schema name (optional, overrides parsed schema if provided)
            database: Database name (optional)
            
        Returns:
            Dictionary with sample data
        """
        full_table_name = self._build_full_table_name(table_name, schema, database)
        query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        
        return self.execute_query(query)
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a safe SELECT query and return results.
        
        Args:
            query: SQL SELECT statement
            
        Returns:
            Dictionary with query results and metadata
        """
        try:
            # Validate the query
            self._validate_query(query)
            
            # Connect to Snowflake
            conn = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                role=self.role,
                warehouse=self.warehouse,
                database=self.database,
                region=self.region
            )
            
            cursor = conn.cursor()
            
            # Execute query
            cursor.execute(query)
            
            # Get results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # Convert results to list of dictionaries
            rows = []
            for row in results:
                row_dict = {}
                for i, col in enumerate(columns):
                    # Handle None values and convert to JSON-serializable format
                    value = row[i]
                    if value is None:
                        value = None
                    elif isinstance(value, (int, float, str, bool)):
                        value = value
                    else:
                        value = str(value)  # Convert other types to string
                    row_dict[col] = value
                rows.append(row_dict)
            
            cursor.close()
            conn.close()
            
            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "query": query
            }
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "query": query
            }
    
    def _parse_model_name(self, model_name: str) -> tuple:
        """
        Parse a DBT model name to extract schema and table name.
        Format: schema__table_name -> (schema, table_name)
        Example: silver__aave_borrows -> (SILVER, aave_borrows)
        """
        if '__' in model_name:
            parts = model_name.split('__', 1)
            if len(parts) == 2:
                schema, table_name = parts
                return schema.upper(), table_name
        return None, model_name
    
    def _build_full_table_name(self, model_name: str, schema: str = None, database: str = None) -> str:
        """
        Build the full table name from a DBT model name or explicit schema/table.
        Handles both DBT naming convention (schema__table) and explicit schema/table.
        """
        parsed_schema, parsed_table = self._parse_model_name(model_name)
        
        # Use parsed schema if available, otherwise use provided schema
        final_schema = parsed_schema if parsed_schema else schema
        final_table = parsed_table
        
        # Build the full name
        full_name = final_table
        if final_schema:
            full_name = f"{final_schema}.{final_table}"
        if database:
            full_name = f"{database}.{full_name}"
        
        return full_name
    
    def get_table_info(self, table_name: str, schema: str = None, database: str = None) -> Dict[str, Any]:
        """
        Get information about a table structure.
        
        Args:
            table_name: Name of the table (can be DBT format like 'silver__aave_borrows')
            schema: Schema name (optional, overrides parsed schema if provided)
            database: Database name (optional, uses current database if not provided)
            
        Returns:
            Dictionary with table information
        """
        parsed_schema, parsed_table = self._parse_model_name(table_name)
        
        # Use provided schema if given, otherwise use parsed schema
        final_schema = schema if schema else parsed_schema
        final_table = parsed_table
        
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{final_table}'
        """
        
        if final_schema:
            query += f" AND TABLE_SCHEMA = '{final_schema}'"
        if database:
            query += f" AND TABLE_CATALOG = '{database}'"
        
        query += " ORDER BY ORDINAL_POSITION"
        
        return self.execute_query(query)

def main():
    """Main function for command-line usage."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python agent_snowflake_client.py 'SELECT * FROM table LIMIT 10'")
        sys.exit(1)
    
    query = sys.argv[1]
    
    try:
        client = AgentSnowflakeClient()
        result = client.execute_query(query)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(json.dumps({"success": False, "error": str(e)}, indent=2))

if __name__ == "__main__":
    main()
