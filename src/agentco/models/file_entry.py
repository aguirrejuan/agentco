"""
File entry models for AgentCo data processing.

This module contains Pydantic models for representing individual file entries
from the FILES directory structure.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, validator


class FileEntry(BaseModel):
    """
    Represents a single file entry from the daily files JSON data.

    Attributes
    ----------
    filename : str
        The name of the uploaded file
    rows : int
        Number of rows processed in the file
    status : str
        Processing status of the file (e.g., 'processed', 'empty', 'failed')
    is_duplicated : bool
        Whether the file is a duplicate
    file_size : Optional[float]
        Size of the file in MB, None for empty files
    uploaded_at : datetime
        Timestamp when the file was uploaded
    status_message : Optional[str]
        Additional status information or error message
    """

    filename: str = Field(..., description="The name of the uploaded file")
    rows: int = Field(..., ge=0, description="Number of rows processed in the file")
    status: str = Field(..., description="Processing status of the file")
    is_duplicated: bool = Field(..., description="Whether the file is a duplicate")
    file_size: Optional[float] = Field(None, description="Size of the file in MB")
    uploaded_at: datetime = Field(
        ..., description="Timestamp when the file was uploaded"
    )
    status_message: Optional[str] = Field(
        None, description="Additional status information"
    )

    @validator("status")
    def validate_status(cls, v: str) -> str:
        """
        Validate that status is one of the expected values.

        Parameters
        ----------
        v : str
            The status value to validate

        Returns
        -------
        str
            The validated status value

        Raises
        ------
        ValueError
            If status is not one of the expected values
        """
        valid_statuses = {"processed", "empty", "failed", "pending"}
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of {valid_statuses}, got: {v}")
        return v

    @validator("file_size")
    def validate_file_size(cls, v: Optional[float], values: dict) -> Optional[float]:
        """
        Validate file size based on status and rows.

        Parameters
        ----------
        v : Optional[float]
            The file size value
        values : dict
            Other field values

        Returns
        -------
        Optional[float]
            The validated file size

        Raises
        ------
        ValueError
            If file size doesn't match expected patterns
        """
        status = values.get("status")
        rows = values.get("rows", 0)

        if status == "empty" and v is not None:
            raise ValueError("Empty files should have file_size=None")
        if status == "processed" and rows > 0 and v is None:
            raise ValueError("Processed files with rows should have file_size")
        if v is not None and v < 0:
            raise ValueError("File size cannot be negative")

        return v

    @property
    def is_empty(self) -> bool:
        """Check if the file is empty."""
        return self.status == "empty" or self.rows == 0

    @property
    def is_processed_successfully(self) -> bool:
        """Check if the file was processed successfully."""
        return self.status == "processed" and not self.is_duplicated

    @property
    def has_data(self) -> bool:
        """Check if the file contains actual data."""
        return self.rows > 0 and self.status == "processed"
