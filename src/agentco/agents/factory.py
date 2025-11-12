"""Detector Agent Factory

This module provides factory functions for creating configured detector agent instances.
"""

from pathlib import Path
from typing import List

from google.adk.agents import LlmAgent, ParallelAgent

from .commons import get_default_tools, get_model
from .detectors import (
    create_duplicated_and_failed_file_detector_agent,
    create_file_upload_after_schedule_detector_agent,
    create_missing_file_detector_agent,
    create_unexpected_empty_file_detector_agent,
    create_unexpected_volume_variation_detector_agent,
    create_upload_of_previous_file_detector_agent,
)


def create_all_detector_agents(
    source_id: str = "195385",
    day_folder: Path = Path(
        "/Users/carlos.aguirre/dev-personal/agentco/artifacts/Files/2025-09-08_20_00_UTC/"
    ),
    datasource_folder: Path = Path(
        "/Users/carlos.aguirre/dev-personal/agentco/artifacts/Files/datasource_cvs/"
    ),
) -> List[LlmAgent]:
    """Create all detector agents with custom configuration.

    Parameters
    ----------
    source_id : str, default="195385"
        The source identifier for the data source toolset
    day_folder : Path, default=Path('...')
        Path to the day folder containing data files
    datasource_folder : Path, default=Path('...')
        Path to the datasource folder containing CV files

    Returns
    -------
    List[LlmAgent]
        List of all configured detector agents
    """
    # Configure tools with custom parameters
    tools = get_default_tools(source_id, day_folder, datasource_folder)

    # Create all detector agents with the configured tools
    agents = [
        create_missing_file_detector_agent(tools),
        create_duplicated_and_failed_file_detector_agent(tools),
        create_unexpected_empty_file_detector_agent(tools),
        create_unexpected_volume_variation_detector_agent(tools),
        create_file_upload_after_schedule_detector_agent(tools),
        create_upload_of_previous_file_detector_agent(tools),
    ]

    return agents


def create_parallel_detection_agent(
    source_id: str = "195385",
    day_folder: Path = Path(
        "/Users/carlos.aguirre/dev-personal/agentco/artifacts/Files/2025-09-08_20_00_UTC/"
    ),
    datasource_folder: Path = Path(
        "/Users/carlos.aguirre/dev-personal/agentco/artifacts/Files/datasource_cvs/"
    ),
) -> ParallelAgent:
    """Create a parallel agent that runs all detectors concurrently.

    Parameters
    ----------
    source_id : str, default="195385"
        The source identifier for the data source toolset
    day_folder : Path, default=Path('...')
        Path to the day folder containing data files
    datasource_folder : Path, default=Path('...')
        Path to the datasource folder containing CV files

    Returns
    -------
    ParallelAgent
        Parallel agent configured with all detector sub-agents
    """
    detector_agents = create_all_detector_agents(
        source_id, day_folder, datasource_folder
    )

    return ParallelAgent(
        name="ParallelDetectionTeam",
        sub_agents=detector_agents,
        description="Runs multiple detection agents in parallel to identify various data quality issues.",
    )
