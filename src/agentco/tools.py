from pathlib import Path
from typing import Dict, List

import duckdb
from google.adk.agents.readonly_context import ReadonlyContext
from google.adk.tools import FunctionTool
from google.adk.tools.base_toolset import BaseToolset

from agentco.logger import logger

from .data.data_converter import DataSourceAnalyzer


class DataSourceToolset(BaseToolset):
    """Toolset that loads and analyzes data source files with singleton pattern for same arguments."""

    # Class-level cache to store instances by their arguments
    _instances: Dict[tuple, "DataSourceToolset"] = {}

    def __new__(
        cls,
        source_id: str,
        day_folder: Path,
        datasource_folder: Path,
        prefix: str = "data_",
    ):
        """
        Implement singleton pattern based on arguments.

        Returns existing instance if one exists with the same arguments,
        otherwise creates a new instance.
        """
        # Create a hashable key from the arguments
        key = (source_id, str(day_folder), str(datasource_folder), prefix)

        # Check if instance already exists
        if key in cls._instances:
            logger.debug(
                f"🔄 Reusing existing DataSourceToolset for source_id={source_id}"
            )
            return cls._instances[key]

        # Create new instance and cache it
        instance = super().__new__(cls)
        cls._instances[key] = instance
        logger.debug(f"🆕 Creating new DataSourceToolset for source_id={source_id}")

        return instance

    @classmethod
    def clear_cache(cls):
        """Clear the singleton cache. Useful for testing or cleanup."""
        cls._instances.clear()
        logger.debug("🧹 DataSourceToolset cache cleared")

    @classmethod
    def get_cache_info(cls) -> Dict[str, int]:
        """Get information about the current cache state."""
        return {
            "cached_instances": len(cls._instances),
            "cached_keys": list(cls._instances.keys()),
        }

    """Toolset that loads and analyzes data source files."""

    def __init__(
        self,
        source_id: str,
        day_folder: Path,
        datasource_folder: Path,
        prefix: str = "data_",
    ):
        """
        Initialize the toolset with paths.

        Args:
            source_id: The source identifier
            day_folder: Path to the day folder containing files
            datasource_folder: Path to the datasource CSV folder
            prefix: Prefix for tool names
        """
        # Skip initialization if already initialized (singleton pattern)
        if hasattr(self, "_initialized"):
            return

        self.source_id = source_id
        self.day_folder = day_folder
        self.datasource_folder = datasource_folder
        self.data = None
        self.markdown_explanation = None
        self.analyzer = None
        self.tool_name_prefix = prefix

        # Initialize tools
        self._query_today_data_tool = FunctionTool(
            self.query_today_data,
        )
        self._query_today_data_tool.name = f"{self.tool_name_prefix}query_today_data"

        self._read_data_source_cv_tool = FunctionTool(
            self.read_data_source_cv,
        )
        self._read_data_source_cv_tool.name = (
            f"{self.tool_name_prefix}read_data_source_cv"
        )

        self._query_today_and_last_weekday_data_tool = FunctionTool(
            self.query_today_and_last_weekday_data,
        )
        self._query_today_and_last_weekday_data_tool.name = (
            f"{self.tool_name_prefix}query_today_and_last_weekday_data"
        )

        self._validate_data_quality_tool = FunctionTool(
            self.validate_data_quality,
        )
        self._validate_data_quality_tool.name = (
            f"{self.tool_name_prefix}validate_data_quality"
        )

        self.analyzer = DataSourceAnalyzer.from_day_folder(
            source_id=self.source_id,
            day_folder=self.day_folder,
            datasource_folder=self.datasource_folder,
        )

        self.data, self.markdown_explanation = self.analyzer.get_data()

        # Create today's data subset
        self.today_data = self.data[self.data["from_period"] == "today"].copy()

        # Initialize DuckDB connections
        self.conn_today = duckdb.connect(":memory:")
        self.conn_today.register("data", self.today_data)

        self.conn_all = duckdb.connect(":memory:")
        self.conn_all.register("data", self.data)

        # Mark as initialized
        self._initialized = True

    async def get_tools(self, context: ReadonlyContext) -> List[FunctionTool]:
        """
        Load the data when tools are initialized.
        This is called once when the agent starts.
        """
        # Initialize analyzer and load data

        # Return the tools that will use this loaded data
        return [
            self._query_today_data_tool,
            self._query_today_and_last_weekday_data_tool,
            self._read_data_source_cv_tool,
            # self._validate_data_quality_tool,
        ]

    def query_today_data(self, sql_query: str) -> str:
        """
        Execute SQL query on today's data only.

        Args:
            sql_query: SQL query to execute against today's files.

        Returns:
            Query results as markdown table or error message.

        Example queries:
            - SELECT COUNT(*) as total_files FROM data;
            - SELECT COUNT(*) as failed FROM data WHERE status = 'FAILED';
            - SELECT source_id, filename, status FROM data WHERE is_duplicated = true LIMIT 10;
            - SELECT filename, rows FROM data WHERE rows = 0;

        Available columns:
            * 'source_id' : str, source identifier
            * 'filename' : str, name of the file
            * 'rows' : int, number of rows in the file
            * 'status' : str, status of the file (e.g., 'SUCCESS', 'FAILED', 'STOPPED')
            * 'is_duplicated' : bool, whether the file is duplicated
            * 'file_size' : int, size of the file in bytes
            * 'uploaded_at' : timestamp, upload timestamp
            * 'status_message' : str, message associated with the status

        Tips:
            - Use aggregations (COUNT, SUM, AVG) to minimize data returned
            - Use LIMIT clause for large result sets
            - Filter by status, is_duplicated, or rows for specific issues
        """
        if self.today_data is None:
            return "Error: Data not loaded. Please initialize the toolset first."

        try:
            result = self.conn_today.query(sql_query).to_df()

            if result.empty:
                return "Query executed successfully but returned no results."

            # Limit output size
            if len(result) > 100:
                return f"Query returned {len(result)} rows. Showing first 100:\n\n{result.head(100).to_markdown()}\n\n... ({len(result) - 100} more rows)"

            return result.to_markdown(index=False)
        except Exception as e:
            return f"Error executing query: {str(e)}\n\nPlease check your SQL syntax and column names."

    def query_today_and_last_weekday_data(self, sql_query: str) -> str:
        """
        Execute SQL query on combined data (today + last weekday).

        Args:
            sql_query: SQL query to execute.

        Returns:
            Query results as markdown table or error message.

        Example queries:
            - SELECT COUNT(*) FROM data WHERE from_period = 'today';
            - SELECT filename, rows, from_period FROM data WHERE from_period IN ('today', 'last_weekday') ORDER BY filename;
            - Compare volumes:
              SELECT
                  t.filename,
                  t.rows as today_rows,
                  l.rows as lastweek_rows,
                  t.rows - l.rows as difference
              FROM
                  (SELECT * FROM data WHERE from_period = 'today') t
              JOIN (SELECT * FROM data WHERE from_period = 'last_weekday') l
              ON t.filename = l.filename;

        Available columns:
            * 'source_id' : str, source identifier
            * 'filename' : str, name of the file
            * 'rows' : int, number of rows in the file
            * 'status' : str, status of the file
            * 'is_duplicated' : bool, whether the file is duplicated
            * 'file_size' : int, size of the file in bytes
            * 'uploaded_at' : timestamp, upload timestamp
            * 'status_message' : str, message associated with the status
            * 'from_period': str, 'today' or 'last_weekday' indicating the data source

        Tips:
            - Always filter by 'from_period' column to distinguish today vs historical
            - Use JOIN to compare same files across periods
            - Use aggregations to minimize data transfer
        """
        if self.data is None:
            return "Error: Data not loaded. Please initialize the toolset first."

        try:
            result = self.conn_all.query(sql_query).to_df()

            if result.empty:
                return "Query executed successfully but returned no results."

            # Limit output size
            if len(result) > 100:
                return f"Query returned {len(result)} rows. Showing first 100:\n\n{result.head(100).to_markdown()}\n\n... ({len(result) - 100} more rows)"

            return result.to_markdown(index=False)
        except Exception as e:
            return f"Error executing query: {str(e)}\n\nPlease check your SQL syntax and column names."

    def read_data_source_cv(self) -> str:
        """
        Read the data source CV (documentation).

        Returns:
            Markdown formatted explanation of the data source including:
            - Expected file patterns and naming conventions
            - Scheduled arrival times
            - Expected volume patterns
            - Normal vs. abnormal behaviors
            - Special cases and exceptions
        """
        if self.markdown_explanation is None:
            return "Error: Data source CV not loaded."

        return self.markdown_explanation

    def validate_data_quality(self) -> str:
        """
        Perform basic data quality checks.

        Returns:
            Summary of data quality metrics including:
            - Total files count
            - Status distribution
            - Duplicate counts
            - Empty file counts
            - Upload time range

        This is useful as a first step before detailed analysis.
        """
        if self.today_data is None:
            return "Error: Data not loaded."

        try:
            checks = self.conn_today.query(
                """
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(DISTINCT source_id) as unique_sources,
                    SUM(CASE WHEN rows = 0 THEN 1 ELSE 0 END) as empty_files,
                    SUM(CASE WHEN is_duplicated THEN 1 ELSE 0 END) as duplicates,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_files,
                    SUM(CASE WHEN status = 'STOPPED' THEN 1 ELSE 0 END) as stopped_files,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_files,
                    MIN(uploaded_at) as earliest_upload,
                    MAX(uploaded_at) as latest_upload,
                    SUM(rows) as total_rows,
                    SUM(file_size) as total_size_bytes
                FROM data
            """
            ).to_df()

            return f"""
Data Quality Summary for Today:
{checks.to_markdown(index=False)}

Quick Insights:
- Success Rate: {(checks['successful_files'].iloc[0] / checks['total_files'].iloc[0] * 100):.1f}%
- Duplicate Rate: {(checks['duplicates'].iloc[0] / checks['total_files'].iloc[0] * 100):.1f}%
- Empty File Rate: {(checks['empty_files'].iloc[0] / checks['total_files'].iloc[0] * 100):.1f}%
"""
        except Exception as e:
            return f"Error in data quality check: {str(e)}"

    async def close(self) -> None:
        """Clean up resources when the agent shuts down."""
        if hasattr(self, "conn_today"):
            self.conn_today.close()
        if hasattr(self, "conn_all"):
            self.conn_all.close()

        self.data = None
        self.markdown_explanation = None
        self.analyzer = None

        logger.debug("✓ Toolset resources cleaned up")
