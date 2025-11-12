from dotenv import load_dotenv
from langfuse import get_client
from openinference.instrumentation.google_adk import GoogleADKInstrumentor

from .cli import app as cli_app
from .data.data_converter import (
    DataSourceAnalyzer,
    get_source_summary,
    load_day_data,
    load_json_to_dataframe,
    load_markdown_explanation,
)
from .tools import DataSourceToolset

load_dotenv()

__all__ = [
    "load_json_to_dataframe",
    "load_day_data",
    "get_source_summary",
    "load_markdown_explanation",
    "DataSourceAnalyzer",
    "cli_app",
    "DataSourceToolset",
]


def main() -> None:
    """Entry point for the CLI application."""
    cli_app()


langfuse = get_client()

# Verify connection
if langfuse.auth_check():
    print("Langfuse client is authenticated and ready!")
else:
    print("Authentication failed. Please check your credentials and host.")


GoogleADKInstrumentor().instrument()
