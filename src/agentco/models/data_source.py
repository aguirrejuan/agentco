"""
Simplified data source model for AgentCo.

This module contains a simple model representing a data source with its
daily files, last weekday files, and markdown explanation.
"""

from typing import List

from pydantic import BaseModel, Field

from .file_entry import FileEntry


class DataSource(BaseModel):
    """
    Simplified data source model containing files and metadata.

    Attributes
    ----------
    source_id : str
        The source identifier (e.g., "195385")
    daily_files : List[FileEntry]
        List of files for the current day
    last_weekday_files : List[FileEntry]
        List of files from the previous weekday
    markdown_explanation : str
        Markdown content explaining the source behavior and patterns
    """

    source_id: str = Field(..., description="Source identifier")
    daily_files: List[FileEntry] = Field(
        default_factory=list, description="Files for current day"
    )
    last_weekday_files: List[FileEntry] = Field(
        default_factory=list, description="Files from last weekday"
    )
    markdown_explanation: str = Field(
        ..., description="Markdown content explaining the source"
    )

    @property
    def total_daily_files(self) -> int:
        """Get total number of files for the current day."""
        return len(self.daily_files)

    @property
    def total_daily_rows(self) -> int:
        """Get total rows processed for the current day."""
        return sum(file_entry.rows for file_entry in self.daily_files)

    @property
    def total_last_weekday_files(self) -> int:
        """Get total number of files from last weekday."""
        return len(self.last_weekday_files)

    @property
    def total_last_weekday_rows(self) -> int:
        """Get total rows processed from last weekday."""
        return sum(file_entry.rows for file_entry in self.last_weekday_files)

    @property
    def has_daily_activity(self) -> bool:
        """Check if there's any activity for the current day."""
        return len(self.daily_files) > 0

    @property
    def daily_processed_files_count(self) -> int:
        """Get count of successfully processed files from current day."""
        return len([f for f in self.daily_files if f.is_processed_successfully])

    @property
    def daily_empty_files_count(self) -> int:
        """Get count of empty files from current day."""
        return len([f for f in self.daily_files if f.is_empty])

    @property
    def daily_failed_files_count(self) -> int:
        """Get count of failed files from current day."""
        return len([f for f in self.daily_files if f.status == "failed"])
