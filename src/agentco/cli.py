"""
CLI interface for AgentCo data analysis agent using Typer.
"""

from pathlib import Path

import typer
from dotenv import load_dotenv
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

# Load environment variables
load_dotenv()

app = typer.Typer(
    name="agentco", help="AgentCo Data Analysis Agent CLI", add_completion=False
)


@app.command()
async def generate_report(
    folder_data: Path = typer.Option(
        ..., exists=True, dir_okay=True, file_okay=False, readable=True
    )
):
    pass


if __name__ == "__main__":
    app()
