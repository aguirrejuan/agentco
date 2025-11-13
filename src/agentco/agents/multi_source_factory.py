"""Multi-Source Data Quality Processing Factory

This module provides factory functions for creating multi-source data quality monitoring pipelines.
Each source is processed independently with its own set of detectors, and results are synthesized
into a comprehensive report.
"""

from pathlib import Path
from typing import Any, Dict, List

from google.adk.agents import LlmAgent, ParallelAgent, SequentialAgent
from google.adk.code_executors import BuiltInCodeExecutor

from ..logger import logger
from .commons import get_model
from .factory import create_all_detector_agents


def create_source_specific_detection_agent(
    source_id: str,
    day_folder: Path,
    datasource_folder: Path,
    agent_name_suffix: str = "",
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
    agent_name_suffix : str, optional
        Suffix to add to agent name for identification

    Returns
    -------
    ParallelAgent
        Parallel agent configured with all detector sub-agents for this source
    """
    # Create detector agents for this specific source
    detector_agents = create_all_detector_agents(
        source_id=source_id, day_folder=day_folder, datasource_folder=datasource_folder
    )

    agent_name = f"SourceDetectionTeam_{source_id}"
    if agent_name_suffix:
        agent_name += f"_{agent_name_suffix}"

    return ParallelAgent(
        name=agent_name,
        sub_agents=detector_agents,
        description=f"Parallel detection team for source {source_id} - runs all detectors concurrently.",
    )


def create_multi_source_detection_pipeline(
    sources_config: List[Dict[str, Any]], synthesis_instructions: str = None
) -> SequentialAgent:
    """Create a pipeline that processes multiple sources independently then synthesizes results.

    Parameters
    ----------
    sources_config : List[Dict[str, Any]]
        List of source configurations. Each dict should contain:
        - source_id: str
        - day_folder: Path
        - datasource_folder: Path
        - name: str (optional, for identification)
    synthesis_instructions : str, optional
        Custom instructions for the synthesis agent. If None, uses default comprehensive instructions.

    Returns
    -------
    SequentialAgent
        Sequential agent that processes all sources in parallel, then synthesizes results
    """

    # Create source-specific detection agents
    source_agents = []
    for config in sources_config:
        source_agent = create_source_specific_detection_agent(
            source_id=config["source_id"],
            day_folder=config["day_folder"],
            datasource_folder=config["datasource_folder"],
            agent_name_suffix=config.get("name", ""),
        )
        source_agents.append(source_agent)

    # Create parallel agent that processes all sources simultaneously
    multi_source_parallel_agent = ParallelAgent(
        name="MultiSourceParallelProcessor",
        sub_agents=source_agents,
        description=f"Processes {len(source_agents)} data sources simultaneously, each with full detection suite.",
    )

    # Create synthesis agent
    if synthesis_instructions is None:
        synthesis_instructions = get_default_synthesis_instructions()

    synthesis_agent = LlmAgent(
        name="MultiSourceSynthesisAgent",
        model=get_model(),
        instruction=synthesis_instructions,
        code_executor=BuiltInCodeExecutor(),
        description="Synthesizes detection results from multiple sources into comprehensive report",
    )

    # Create sequential pipeline: multi-source parallel processing then synthesis
    return SequentialAgent(
        name="MultiSourceDataQualityPipeline",
        sub_agents=[multi_source_parallel_agent, synthesis_agent],
        description=f"Complete data quality pipeline processing {len(source_agents)} sources independently then generating unified report.",
    )


def get_default_synthesis_instructions() -> str:
    """Get default synthesis instructions for multi-source reporting.

    Returns
    -------
    str
        Default synthesis instructions optimized for multi-source reports in executive format
    """
    return """
MISSION: Generate executive-level data quality report consolidating detection results from all sources.

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
