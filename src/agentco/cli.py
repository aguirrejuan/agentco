"""
CLI interface for AgentCo data analysis agent using Typer.
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from .agents.factory import (
    create_auto_discovery_multi_source_config,
    create_multi_source_detection_pipeline,
)
from .logger import logger

# Load environment variables
load_dotenv()

app = typer.Typer(
    name="agentco", help="AgentCo Data Analysis Agent CLI", add_completion=False
)


@app.command()
def analyze(
    cv_folder: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=True,
        file_okay=False,
        readable=True,
        help="Path to folder containing CV files (*_native.md)",
    ),
    json_folder: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=True,
        file_okay=False,
        readable=True,
        help="Path to folder containing JSON data files",
    ),
    extract_names: bool = typer.Option(
        True,
        "--extract-names/--no-extract-names",
        help="Extract source names from CV file headers",
    ),
    max_sources: Optional[int] = typer.Option(
        None,
        "--max-sources",
        "-m",
        help="Maximum number of sources to analyze (for testing)",
        min=1,
    ),
    save_output: bool = typer.Option(
        False,
        "--save-output/--no-save-output",
        help="Whether to save the output report to a file",
    ),
    session_id: str = typer.Option(
        None,
        "--session-id",
        "-s",
        help="Custom session ID (default: auto-generated)",
    ),
):
    """
    Run data quality analysis on specified folders.

    Analyzes CV files and JSON data to generate executive data quality reports.

    Example:
        agentco analyze /path/to/cv_files /path/to/json_files
        agentco analyze ./cv_files ./json_files --max-sources 3 --output json
    """
    # Generate session ID if not provided
    if session_id is None:
        session_id = f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    typer.echo("🔍 AgentCo Data Quality Analysis")
    typer.echo("=" * 50)

    # Run the analysis
    asyncio.run(
        _run_analysis(
            cv_folder=cv_folder,
            json_folder=json_folder,
            extract_names=extract_names,
            max_sources=max_sources,
            save_output=save_output,
            session_id=session_id,
        )
    )


async def _run_analysis(
    cv_folder: Path,
    json_folder: Path,
    extract_names: bool,
    max_sources: Optional[int],
    save_output: bool,
    session_id: str,
) -> None:
    """
    Internal function to run the data quality analysis.

    Parameters
    ----------
    cv_folder : Path
        Path to folder containing CV files
    json_folder : Path
        Path to folder containing JSON data files
    extract_names : bool
        Whether to extract source names from CV files
    max_sources : Optional[int]
        Maximum number of sources to analyze
    save_output: bool
        Whether to save output to a file
    session_id : str
        Session identifier
    """
    try:
        # Display input information
        typer.echo(f"📁 CV Files Folder: {cv_folder}")
        typer.echo(f"📁 JSON Files Folder: {json_folder}")
        typer.echo(f"🏷️  Extract Names: {extract_names}")
        if max_sources:
            typer.echo(f"🔢 Max Sources: {max_sources}")
        typer.echo(f"📋 Session ID: {session_id}")
        typer.echo()

        # Step 1: Auto-discover sources
        typer.echo("🔍 Discovering data sources...")
        sources = create_auto_discovery_multi_source_config(
            datasource_folder=cv_folder,
            json_files_folder=json_folder,
            extract_names_from_cv=extract_names,
        )

        # Limit sources if specified
        if max_sources and len(sources) > max_sources:
            sources = sources[:max_sources]
            typer.echo(f"⚠️  Limited to {max_sources} sources for analysis")

        typer.echo(f"✅ Discovered {len(sources)} sources:")
        for i, source in enumerate(sources, 1):
            typer.echo(f"   {i:2d}. {source['name']} (ID: {source['source_id']})")
        typer.echo()

        # Step 2: Create pipeline
        typer.echo("🚀 Creating data quality pipeline...")
        pipeline = create_multi_source_detection_pipeline(sources)

        typer.echo(f"✅ Pipeline created:")
        typer.echo(f"   • {len(sources)} sources")
        typer.echo(f"   • {len(sources) * 6} detectors (6 per source)")
        typer.echo(f"   • Executive format reports")
        typer.echo(f"   • Parallel processing")
        typer.echo()

        # Step 3: Setup ADK runner
        typer.echo("⚙️  Setting up analysis session...")
        session_service = InMemorySessionService()
        runner = Runner(
            agent=pipeline, app_name="agentco_cli", session_service=session_service
        )

        # Create session
        session = await session_service.create_session(
            app_name="agentco_cli",
            user_id="cli_user",
            session_id=session_id,
        )
        typer.echo(f"✅ Session created: {session_id}")
        typer.echo()

        # Step 4: Run analysis
        typer.echo("🔬 Running data quality analysis...")
        typer.echo(
            "   This may take a few minutes depending on the number of sources..."
        )

        content = types.Content(
            role="user",
            parts=[types.Part(text="Generate executive data quality report")],
        )

        events = runner.run(
            user_id="cli_user",
            session_id=session_id,
            new_message=content,
        )

        # Step 5: Process results
        report_found = False
        for event in events:
            logger.debug(f"Received event: {type(event).__name__}")
            logger.debug(f"Event content: {event}")

            if (
                event.author == "MultiSourceSynthesisAgent"
                and event.finish_reason is not None
            ):
                logger.info(f"Final event from MultiSourceSynthesisAgent: {event}")
                report = event.content.parts[0].text.strip()
                logger.info(f"Extracted report: {report}")
                report_found = True

        if not report_found:
            typer.echo("⚠️  No report generated. Check logs for details.", err=True)
            raise typer.Exit(1)

        # Output the report
        if save_output:
            output_file = Path(f"data_quality_report_{session_id}.md")
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(report)
            typer.echo(f"💾 Report saved to {output_file}")

        typer.echo()
        typer.echo("✅ Analysis completed successfully!")

    except FileNotFoundError as e:
        typer.echo(f"❌ File not found: {e}", err=True)
        raise typer.Exit(1)
    except ValueError as e:
        typer.echo(f"❌ Configuration error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        typer.echo(f"❌ Analysis failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def info():
    """
    Display AgentCo CLI information.
    """
    typer.echo("AgentCo CLI - Data Quality Analysis Agent")
    typer.echo("Version: 1.0.0")
    typer.echo("Author: AgentCo Team")
    typer.echo("For help, use the --help option with any command.")


if __name__ == "__main__":
    app()
