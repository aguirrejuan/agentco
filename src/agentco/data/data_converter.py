"""
Simple JSON to DataFrame converter for AgentCo data.
"""

import json
from pathlib import Path
from typing import Dict, Tuple, Union

import pandas as pd


def load_json_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Load JSON file and convert to DataFrame.

    Parameters
    ----------
    file_path : str
        Path to JSON file

    Returns
    -------
    pd.DataFrame
        DataFrame with flattened data
    """
    with open(file_path, "r") as f:
        data = json.load(f)

    # Flatten nested JSON structure
    rows = []
    for source_id, files in data.items():
        for file_data in files:
            row = {"source_id": source_id, **file_data}
            rows.append(row)

    df = pd.DataFrame(rows)

    # Convert datetime column if it exists
    if "uploaded_at" in df.columns:
        df["uploaded_at"] = pd.to_datetime(df["uploaded_at"])

    return df


def load_day_data(day_folder: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load both files.json and files_last_weekday.json from a day folder.

    Parameters
    ----------
    day_folder : str
        Path to day folder (e.g., "artifacts/Files/2025-09-08_20_00_UTC/")

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        (daily_files_df, last_weekday_files_df)
    """
    folder_path = Path(day_folder)

    # Load daily files
    daily_df = load_json_to_dataframe(folder_path / "files.json")

    # Load last weekday files
    last_weekday_df = load_json_to_dataframe(folder_path / "files_last_weekday.json")

    return daily_df, last_weekday_df


def get_source_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get summary statistics by source ID.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with file data

    Returns
    -------
    pd.DataFrame
        Summary statistics by source_id including total_files, total_rows,
        processed_files, empty_files, failed_files, avg_file_size
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "source_id",
                "total_files",
                "total_rows",
                "processed_files",
                "empty_files",
                "failed_files",
                "avg_file_size",
            ]
        )

    summary = (
        df.groupby("source_id")
        .agg({"filename": "count", "rows": "sum", "file_size": "mean"})
        .rename(
            columns={
                "filename": "total_files",
                "rows": "total_rows",
                "file_size": "avg_file_size",
            }
        )
    )

    # Count files by status
    status_counts = df.groupby(["source_id", "status"]).size().unstack(fill_value=0)

    # Add status columns to summary
    summary["processed_files"] = status_counts.get("processed", 0)
    summary["empty_files"] = status_counts.get("empty", 0)
    summary["failed_files"] = status_counts.get("failed", 0)

    return summary.reset_index()


def load_markdown_explanation(
    source_id: str, datasource_folder: Union[str, Path]
) -> str:
    """
    Load the markdown explanation for a specific source.

    Parameters
    ----------
    source_id : str
        The source identifier (e.g., "195385")
    datasource_folder : Union[str, Path]
        Path to the datasource_cvs folder

    Returns
    -------
    str
        The markdown content

    Raises
    ------
    FileNotFoundError
        If the markdown file doesn't exist
    """
    datasource_folder = Path(datasource_folder)
    md_file = datasource_folder / f"{source_id}_native.md"

    if not md_file.exists():
        raise FileNotFoundError(f"Markdown file not found: {md_file}")

    with open(md_file, "r", encoding="utf-8") as f:
        return f.read()


class DataSourceAnalyzer:
    """
    A class to analyze data source information combining DataFrames and markdown.

    Attributes
    ----------
    source_id : str
        The source identifier
    daily_files_df : pd.DataFrame
        DataFrame with daily files data
    last_weekday_df : pd.DataFrame
        DataFrame with last weekday files data
    markdown_explanation : str
        Markdown content explaining the source
    """

    def __init__(
        self,
        source_id: str,
        data: pd.DataFrame,
        markdown_explanation: str,
    ):
        """
        Initialize the DataSourceAnalyzer.

        Parameters
        ----------
        source_id : str
            The source identifier
        daily_files_df : pd.DataFrame
            DataFrame with daily files data
        last_weekday_df : pd.DataFrame
            DataFrame with last weekday files data
        markdown_explanation : str
            Markdown content explaining the source
        """
        self.source_id = source_id
        self.data = data
        self.markdown_explanation = markdown_explanation

    @classmethod
    def from_day_folder(
        cls,
        source_id: str,
        day_folder: Union[str, Path],
        datasource_folder: Union[str, Path],
    ) -> "DataSourceAnalyzer":
        """
        Create DataSourceAnalyzer from a day folder.

        Parameters
        ----------
        source_id : str
            The source identifier
        day_folder : Union[str, Path]
            Path to the day folder
        datasource_folder : Union[str, Path]
            Path to the datasource_cvs folder

        Returns
        -------
        DataSourceAnalyzer
            Initialized analyzer instance
        """
        daily_df, last_weekday_df = load_day_data(day_folder)

        # Filter by source_id
        daily_source_df = daily_df[daily_df["source_id"] == source_id].copy()
        last_weekday_source_df = last_weekday_df[
            last_weekday_df["source_id"] == source_id
        ].copy()

        daily_source_df["from"] = "today"
        last_weekday_source_df["from"] = "last_weekday"

        data = pd.concat([daily_source_df, last_weekday_source_df], ignore_index=True)

        markdown_content = load_markdown_explanation(source_id, datasource_folder)

        return cls(source_id, data, markdown_content)

    def get_data(self) -> pd.DataFrame:
        """
        Get combined DataFrame for the source.

        Returns
        -------
        pd.DataFrame
            Combined DataFrame with daily and last weekday data
        """
        return (
            self.data,
            self.markdown_explanation,
        )
