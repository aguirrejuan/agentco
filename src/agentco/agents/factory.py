"""Detector Agent Factory

This module provides factory functions for creating configured detector agent instances,
including multi-source processing capabilities following ADK best practices.
"""

from pathlib import Path
from typing import Any, Dict, List

from google.adk.agents import LlmAgent, ParallelAgent, SequentialAgent

from ..logger import logger
from .commons import get_model, get_tools
from .detectors import (
    create_duplicated_and_failed_file_detector_agent,
    create_file_upload_after_schedule_detector_agent,
    create_missing_file_detector_agent,
    create_source_synthesizer_agent,
    create_unexpected_empty_file_detector_agent,
    create_unexpected_volume_variation_detector_agent,
    create_upload_of_previous_file_detector_agent,
)


def create_all_detector_agents(
    source_id: str, day_folder: Path, datasource_folder: Path
) -> List[LlmAgent]:
    """Create all detector agents with custom configuration.

    Parameters
    ----------
    source_id : str
        The source identifier for the data source toolset
    day_folder : Path
        Path to the day folder containing data files
    datasource_folder : Path
        Path to the datasource folder containing CV files

    Returns
    -------
    List[LlmAgent]
        List of all configured detector agents
    """
    # Configure tools with custom parameters
    tools = get_tools(source_id, day_folder, datasource_folder)

    # Create all detector agents with the configured tools and source-specific keys
    agents = [
        create_missing_file_detector_agent(tools, source_id),
        create_duplicated_and_failed_file_detector_agent(tools, source_id),
        create_unexpected_empty_file_detector_agent(tools, source_id),
        create_unexpected_volume_variation_detector_agent(tools, source_id),
        create_file_upload_after_schedule_detector_agent(tools, source_id),
        create_upload_of_previous_file_detector_agent(tools, source_id),
    ]

    return agents


def create_parallel_detection_agent(
    source_id: str, day_folder: Path, datasource_folder: Path
) -> ParallelAgent:
    """Create a parallel agent that runs all detectors concurrently.

    Parameters
    ----------
    source_id : str,
        The source identifier for the data source toolset
    day_folder : Path,
        Path to the day folder containing data files
    datasource_folder : Path
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


def create_source_specific_detection_agent(
    source_id: str, day_folder: Path, datasource_folder: Path, source_name: str = ""
) -> ParallelAgent:
    """Create a parallel detection agent for a specific source.

    Parameters
    ----------
    source_id : str
        The source identifier for the data source toolset
    day_folder : Path
        Path to the day folder containing data files
    datasource_folder : Path
        Path to the datasource folder containing CV files
    source_name : str, optional
        Human-readable name for the source (for identification)

    Returns
    -------
    ParallelAgent
        Parallel agent configured with all detector sub-agents for this source
    """

    def sanitize_agent_name(name: str) -> str:
        """Sanitize name for use in agent identifiers."""
        # Replace spaces and special characters with underscores
        import re

        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        # Remove multiple consecutive underscores
        sanitized = re.sub(r"_+", "_", sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip("_")
        return sanitized

    # Create detector agents for this specific source
    detector_agents = create_all_detector_agents(
        source_id=source_id, day_folder=day_folder, datasource_folder=datasource_folder
    )

    agent_name = f"SourceDetectionTeam_{source_id}"
    if source_name:
        sanitized_name = sanitize_agent_name(source_name)
        if sanitized_name:
            agent_name += f"_{sanitized_name}"

    return ParallelAgent(
        name=agent_name,
        sub_agents=detector_agents,
        description=f"Parallel detection team for source {source_id} ({source_name}) - runs all detectors concurrently.",
    )


def create_source_analysis_pipeline(
    source_id: str, day_folder: Path, datasource_folder: Path, source_name: str = ""
) -> SequentialAgent:
    """Create a complete analysis pipeline for a specific source with parallel detection followed by synthesis.

    This creates a sequential pipeline that:
    1. Runs all 6 detectors in parallel for comprehensive analysis
    2. Synthesizes results into a structured source report

    Parameters
    ----------
    source_id : str
        The source identifier for the data source toolset
    day_folder : Path
        Path to the day folder containing data files
    datasource_folder : Path
        Path to the datasource folder containing CV files
    source_name : str, optional
        Human-readable name for the source (for identification)

    Returns
    -------
    SequentialAgent
        Sequential agent that runs parallel detection then synthesis
    """
    logger.debug(
        f"Creating source analysis pipeline for source_id={source_id}, source_name={source_name}"
    )
    # Create parallel detection agent first (this will create the tools)
    parallel_detection_agent = create_source_specific_detection_agent(
        source_id=source_id,
        day_folder=day_folder,
        datasource_folder=datasource_folder,
        source_name=source_name,
    )

    logger.debug(
        f"Created parallel detection agent for source_id={source_id}, source_name={source_name}"
    )

    # Create source synthesizer agent with unique output key for multi-source synthesis
    source_report_key = f"source_report_{source_id}"
    synthesizer_agent = create_source_synthesizer_agent(
        output_key=source_report_key, source_id=source_id
    )

    logger.debug(
        f"Created source synthesizer agent for source_id={source_id}, source_name={source_name}, output_key={source_report_key}"
    )

    def sanitize_agent_name(name: str) -> str:
        """Sanitize name for use in agent identifiers."""
        import re

        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        sanitized = re.sub(r"_+", "_", sanitized)
        return sanitized.strip("_")

    # Create agent name
    agent_name = f"SourceAnalysisPipeline_{source_id}"
    if source_name:
        sanitized_name = sanitize_agent_name(source_name)
        if sanitized_name:
            agent_name += f"_{sanitized_name}"

    logger.debug(f"Final agent name for pipeline: {agent_name}")

    return SequentialAgent(
        name=agent_name,
        sub_agents=[parallel_detection_agent, synthesizer_agent],
        description=f"Complete analysis pipeline for source {source_id} ({source_name}) - parallel detection followed by synthesis report.",
    )


def create_multi_source_detection_pipeline(
    sources_config: List[Dict[str, Any]], synthesis_instructions: str = None
) -> SequentialAgent:
    """Create a pipeline that processes multiple sources independently then synthesizes results.

    This follows the ADK pattern from the parallel research example, where multiple independent
    agents run in parallel, then a synthesis agent combines their results.

    Each source now gets its own analysis pipeline that includes:
    1. Parallel detection (6 detectors running concurrently)
    2. Source-specific synthesis (produces individual source reports)
    3. Final multi-source synthesis (combines all source reports into executive summary)

    Parameters
    ----------
    sources_config : List[Dict[str, Any]]
        List of source configurations. Each dict should contain:
        - source_id: str
        - day_folder: Path
        - datasource_folder: Path
        - name: str (optional, for identification)
    synthesis_instructions : str, optional
        Custom instructions for the synthesis agent. If None, uses default multi-source instructions.

    Returns
    -------
    SequentialAgent
        Sequential agent that processes all sources in parallel, then synthesizes results
    """

    # Create source-specific detection agents (temporarily without synthesis to debug)
    source_pipelines = []
    for config in sources_config:
        # Use the old working approach temporarily
        source_pipeline = create_source_analysis_pipeline(
            source_id=config["source_id"],
            day_folder=config["day_folder"],
            datasource_folder=config["datasource_folder"],
            source_name=config.get("name", ""),
        )
        source_pipelines.append(source_pipeline)

    # Create parallel agent that processes all source pipelines simultaneously
    multi_source_parallel_agent = ParallelAgent(
        name="MultiSourceParallelProcessor",
        sub_agents=source_pipelines,
        description=f"Processes {len(source_pipelines)} data sources simultaneously, each with full detection + synthesis pipeline.",
    )

    # Create final synthesis agent with multi-source instructions
    if synthesis_instructions is None:
        synthesis_instructions = get_default_multi_source_synthesis_instructions()

    # Build dynamic instruction that includes all source report keys
    source_report_keys = [
        f"source_report_{config['source_id']}" for config in sources_config
    ]
    source_reports_section = "\n".join(
        [
            f"- **Source {config['source_id']} ({config.get('name', 'Unknown')})**: {{source_report_{config['source_id']}}}"
            for config in sources_config
        ]
    )

    dynamic_instruction = f"""
INPUT CONTEXT:
You will receive individual source reports from multiple data sources that have been analyzed independently. Each source went through parallel detection and individual synthesis.

**Individual Source Reports:**
{source_reports_section}

{synthesis_instructions}
"""
    logger.debug(
        f"Creating final multi-source synthesis agent with instruction:\n{dynamic_instruction}"
    )
    final_synthesis_agent = LlmAgent(
        name="MultiSourceFinalSynthesisAgent",
        model=get_model(),
        instruction=dynamic_instruction,
        description="Synthesizes individual source reports into comprehensive executive-level cross-source report",
    )

    # Create sequential pipeline: multi-source parallel processing then final synthesis
    return SequentialAgent(
        name="MultiSourceDataQualityPipeline",
        sub_agents=[multi_source_parallel_agent, final_synthesis_agent],
        description=f"Complete data quality pipeline processing {len(source_pipelines)} sources independently with individual reports then generating unified executive report.",
    )


def create_single_source_complete_analysis(
    source_id: str, day_folder: Path, datasource_folder: Path, source_name: str = ""
) -> SequentialAgent:
    """Create a complete single-source analysis pipeline with detection and synthesis.

    This is a convenience function for analyzing a single data source with:
    1. Parallel detection using all 6 detectors
    2. Source synthesis to produce a comprehensive report

    Parameters
    ----------
    source_id : str
        The source identifier for the data source toolset
    day_folder : Path
        Path to the day folder containing data files
    datasource_folder : Path
        Path to the datasource folder containing CV files
    source_name : str, optional
        Human-readable name for the source (for identification)

    Returns
    -------
    SequentialAgent
        Complete single-source analysis pipeline
    """
    return create_source_analysis_pipeline(
        source_id=source_id,
        day_folder=day_folder,
        datasource_folder=datasource_folder,
        source_name=source_name,
    )


def get_default_multi_source_synthesis_instructions() -> str:
    """Get default synthesis instructions optimized for multi-source reporting.

    Returns
    -------
    str
        Default synthesis instructions for cross-source analysis and reporting
    """
    return """
MISSION: Generate executive-level data quality report consolidating individual source reports.

REQUIRED FORMAT - MATCH EXACTLY:
*Report generated at UTC HOUR*: HH:MM UTC

* Urgent Action Required*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: [issue details] → *Action:* [specific steps]

* Needs Attention*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: [issue details] → *Action:* [if needed]

* No Action Needed*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: `[X,XXX] records`
• All other recent files appear normal

CLASSIFICATION:
**Urgent**: 2+ missing files, failed processing, >100% volume change, processing blocks
**Attention**: 1 missing file, 50-100% volume change, >4h delays, schedule changes  
**Normal**: All received, volumes ±50%, no failures/duplicates

FORMATTING:
- Use • bullets and *text* for bold (NOT **text**)
- Format: → *Action:* for recommendations
- Records: `[X,XXX] records` in backticks
- Include entities, time windows, specific counts
- No markdown, emojis, or extra sections

ACTIONS:
- Missing files: "Notify provider to generate/re-send; re-run ingestion and verify completeness"
- Schedule changes: "Confirm schedule change; adjust downstream triggers if needed"
- Volume anomalies: "Confirm coverage/window; monitor next run"
"""


def create_auto_discovery_multi_source_config(
    datasource_folder: Path, json_files_folder: Path, extract_names_from_cv: bool = True
) -> List[Dict[str, Any]]:
    """Automatically discover all CV files and create multi-source configuration.

    Parameters
    ----------
    datasource_folder : Path
        Path to folder containing all CV files (will scan for *_native.md files)
    json_files_folder : Path
        Path to folder containing JSON files with data
    extract_names_from_cv : bool, default=True
        If True, extracts source names directly from CV file headers
        If False, uses generic "Source_XXXXX" naming

    Returns
    -------
    List[Dict[str, Any]]
        Auto-generated source configurations based on discovered CV files
    """
    if not datasource_folder.exists():
        raise FileNotFoundError(f"Datasource folder not found: {datasource_folder}")

    if not json_files_folder.exists():
        raise FileNotFoundError(f"JSON files folder not found: {json_files_folder}")

    # Discover all *_native.md files (CV files)
    cv_files = list(datasource_folder.glob("*_native.md"))

    if not cv_files:
        raise ValueError(f"No CV files (*_native.md) found in {datasource_folder}")

    def extract_source_name_from_cv(cv_file_path: Path) -> str:
        """Extract source name from CV file header."""
        try:
            with open(cv_file_path, "r", encoding="utf-8") as f:
                first_line = f.readline().strip()
                # Extract name from markdown header: "# _Settlement_Layout_2" -> "Settlement_Layout_2"
                if first_line.startswith("#"):
                    # Remove # and any leading/trailing underscores and whitespace
                    name = first_line.lstrip("#").strip().strip("_")
                    return (
                        name
                        if name
                        else f"Source_{cv_file_path.stem.replace('_native', '')}"
                    )
                else:
                    # Fallback if no header found
                    return f"Source_{cv_file_path.stem.replace('_native', '')}"
        except Exception as e:
            logger.warning(f"Warning: Could not read CV file {cv_file_path}: {e}")
            return f"Source_{cv_file_path.stem.replace('_native', '')}"

    sources_config = []

    for cv_file in sorted(cv_files):
        # Extract source ID from filename (e.g., "195385_native.md" -> "195385")
        source_id = cv_file.stem.replace("_native", "")

        # Extract source name from CV file or use generic name
        if extract_names_from_cv:
            source_name = extract_source_name_from_cv(cv_file)
        else:
            source_name = f"Source_{source_id}"

        config = {
            "source_id": source_id,
            "name": source_name,
            "day_folder": json_files_folder,
            "datasource_folder": datasource_folder,
        }

        sources_config.append(config)

    return sources_config
