"""Unit tests for database utilities."""
import pytest
import psycopg2
from unittest.mock import patch, MagicMock, PropertyMock

from src.utils.db import (
    get_db_connection,
    execute_query,
    execute_many,
    table_exists
)


@pytest.fixture
def mock_connection():
    """Mock psycopg2 connection for testing."""
    conn = MagicMock(spec=psycopg2.extensions.connection)
    # Use PropertyMock for the 'closed' attribute to ensure it stays False
    type(conn).closed = PropertyMock(return_value=False)
    conn.autocommit = False
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


class TestGetDBConnection:
    """Tests for get_db_connection context manager."""
    
    @patch('src.utils.db.psycopg2.connect')
    def test_connection_success(self, mock_connect, mock_connection):
        """Test successful connection and commit."""
        conn, cursor = mock_connection
        mock_connect.return_value = conn
        
        with get_db_connection() as db_conn:
            assert db_conn == conn
        
        # Verify connection lifecycle
        mock_connect.assert_called_once()
        conn.commit.assert_called_once()
        conn.close.assert_called_once()
    
    @patch('src.utils.db.psycopg2.connect')
    def test_autocommit_mode(self, mock_connect, mock_connection):
        """Test autocommit mode doesn't call commit()."""
        conn, cursor = mock_connection
        mock_connect.return_value = conn
        
        with get_db_connection(autocommit=True) as db_conn:
            assert db_conn.autocommit is True
        
        # Should NOT call commit() in autocommit mode
        conn.commit.assert_not_called()
        conn.close.assert_called_once()
    
    @patch('src.utils.db.psycopg2.connect')
    def test_rollback_on_error(self, mock_connect, mock_connection):
        """Test rollback occurs when exception is raised."""
        conn, cursor = mock_connection
        mock_connect.return_value = conn
        
        with pytest.raises(psycopg2.DatabaseError):
            with get_db_connection() as db_conn:
                # Simulate a database error
                raise psycopg2.DatabaseError("Test database error")
        
        # Verify rollback called, not commit
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        conn.close.assert_called_once()
    
    @patch('src.utils.db.psycopg2.connect')
    def test_connection_failure(self, mock_connect):
        """Test handling of connection failure."""
        mock_connect.side_effect = psycopg2.OperationalError("Connection refused")
        
        with pytest.raises(psycopg2.OperationalError):
            with get_db_connection() as conn:
                pass


class TestExecuteQuery:
    """Tests for execute_query convenience function."""
    
    @patch('src.utils.db.get_db_connection')
    def test_execute_without_fetch(self, mock_get_conn, mock_connection):
        """Test query execution without fetching results."""
        conn, cursor = mock_connection
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
        
        result = execute_query("UPDATE table SET value = %s", (42,))
        
        cursor.execute.assert_called_once_with("UPDATE table SET value = %s", (42,))
        cursor.fetchall.assert_not_called()
        assert result is None
    
    @patch('src.utils.db.get_db_connection')
    def test_execute_with_fetch(self, mock_get_conn, mock_connection):
        """Test query execution with result fetching."""
        conn, cursor = mock_connection
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
        
        expected_rows = [(1, 'Alice'), (2, 'Bob')]
        cursor.fetchall.return_value = expected_rows
        
        result = execute_query("SELECT * FROM users", fetch=True)
        
        cursor.execute.assert_called_once()
        cursor.fetchall.assert_called_once()
        assert result == expected_rows


class TestExecuteMany:
    """Tests for execute_many bulk operation function."""
    
    @patch('src.utils.db.get_db_connection')
    @patch('src.utils.db.psycopg2.extras.execute_batch')
    def test_bulk_insert(self, mock_batch, mock_get_conn, mock_connection):
        """Test bulk insert operation."""
        conn, cursor = mock_connection
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
        cursor.rowcount = 3
        
        data = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
        query = "INSERT INTO users (id, name) VALUES (%s, %s)"
        
        rows_affected = execute_many(query, data)
        
        mock_batch.assert_called_once_with(cursor, query, data)
        assert rows_affected == 3


class TestTableExists:
    """Tests for table_exists utility function."""
    
    @patch('src.utils.db.execute_query')
    def test_table_exists_true(self, mock_execute):
        """Test when table exists."""
        mock_execute.return_value = [(True,)]
        
        result = table_exists('staging', 'trip_raw')
        
        assert result is True
        mock_execute.assert_called_once()
    
    @patch('src.utils.db.execute_query')
    def test_table_exists_false(self, mock_execute):
        """Test when table does not exist."""
        mock_execute.return_value = [(False,)]
        
        result = table_exists('staging', 'nonexistent_table')
        
        assert result is False
    
    @patch('src.utils.db.execute_query')
    def test_table_exists_query_failure(self, mock_execute):
        """Test handling of query failure."""
        mock_execute.return_value = None
        
        result = table_exists('staging', 'trip_raw')
        
        assert result is False


# Integration test (requires running database)
@pytest.mark.integration
class TestDBIntegration:
    """Integration tests requiring actual database connection."""
    
    def test_real_connection(self):
        """Test actual database connection (only runs if DB available)."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    assert result == (1,)
        except psycopg2.OperationalError:
            pytest.skip("Database not available for integration test")
