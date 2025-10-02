"""Database connection utilities with context manager pattern."""
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Generator
import logging

from src.config import db_config

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection(autocommit: bool = False) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager for database connections.
    
    Automatically handles connection lifecycle:
    - Opens connection
    - Commits on success (if not autocommit)
    - Rolls back on error
    - Always closes connection
    
    Args:
        autocommit: If True, sets connection to autocommit mode.
                   Use for DDL statements (CREATE TABLE, etc.)
    
    Yields:
        psycopg2 connection object
        
    Example:
        >>> with get_db_connection() as conn:
        ...     with conn.cursor() as cur:
        ...         cur.execute("INSERT INTO table VALUES (%s)", (value,))
        # Connection auto-commits and closes here
        
    Raises:
        psycopg2.Error: If connection fails or query errors occur
    """
    conn = None
    try:
        # Create connection using config
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            database=db_config.database,
        )
        
        # Set autocommit mode if requested
        conn.autocommit = autocommit
        
        logger.debug(
            f"Database connection established: {db_config.host}:{db_config.port}/{db_config.database}"
        )
        
        yield conn
        
        # Commit if not in autocommit mode and no exceptions occurred
        if not autocommit and not conn.closed:
            conn.commit()
            logger.debug("Transaction committed successfully")
            
    except psycopg2.Error as e:
        # Rollback on any database error
        if conn and not conn.closed and not autocommit:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
        raise
        
    finally:
        # Always close connection
        if conn and not conn.closed:
            conn.close()
            logger.debug("Database connection closed")


def execute_query(query: str, params: tuple = None, fetch: bool = False) -> list | None:
    """
    Execute a single query with automatic connection management.
    
    Convenience function for simple queries that don't need explicit
    connection handling.
    
    Args:
        query: SQL query to execute
        params: Query parameters (for parameterized queries)
        fetch: If True, returns query results
        
    Returns:
        List of tuples if fetch=True, None otherwise
        
    Example:
        >>> rows = execute_query("SELECT * FROM table WHERE id = %s", (123,), fetch=True)
        >>> execute_query("UPDATE table SET value = %s WHERE id = %s", (val, id))
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
    return None


def execute_many(query: str, data: list[tuple]) -> int:
    """
    Execute a query with multiple parameter sets (bulk insert/update).
    
    More efficient than individual execute() calls for batch operations.
    
    Args:
        query: SQL query with parameter placeholders
        data: List of tuples, each containing parameters for one execution
        
    Returns:
        Number of rows affected
        
    Example:
        >>> data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
        >>> execute_many("INSERT INTO users (id, name) VALUES (%s, %s)", data)
        3
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, query, data)
            return cur.rowcount


def table_exists(schema: str, table: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        schema: Schema name (e.g., 'staging', 'warehouse')
        table: Table name
        
    Returns:
        True if table exists, False otherwise
        
    Example:
        >>> if table_exists('staging', 'trip_raw'):
        ...     print("Table exists!")
    """
    query = """
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        );
    """
    result = execute_query(query, (schema, table), fetch=True)
    return result[0][0] if result else False
