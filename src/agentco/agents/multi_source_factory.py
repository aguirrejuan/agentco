"""Multi-Source Data Quality Processing Factory

This module provides factory functions for creating multi-source data quality monitoring pipelines.
Each source is processed independently with its own set of detectors, and results are synthesized
into a comprehensive report.
"""

from pathlib import Path
from typing import Any, Dict, List

from google.adk.agents import LlmAgent, ParallelAgent, SequentialAgent

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
MISSION: Generate an executive-level data quality monitoring report in the exact format specified, consolidating detection findings from ALL sources processed independently.

INPUT PROCESSING:
You will receive detection results from multiple data sources. Each source was processed independently with its own complete set of 6 detectors (missing files, duplicates/failures, empty files, volume variations, late uploads, previous period files). Synthesize these into the EXACT format shown below.

REQUIRED REPORT FORMAT - MATCH EXACTLY:
```
*Report generated at UTC HOUR*: HH:MM UTC

* Urgent Action Required*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: [detailed issue description with specifics] → *Action:* [specific action steps]

* Needs Attention*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: [detailed issue description with specifics] → *Action:* [specific action steps if needed]

* No Action Needed*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: `[X,XXX] records`
• All other recent files appear normal
```

CRITICALITY CLASSIFICATION:

* Urgent Action Required* - Sources with:
- 2+ files missing from same source  
- Critical duplicated/failed files blocking processing
- Volume changes >100% increase or >80% decrease
- Multiple files failed with same error pattern
- Any processing-blocking issues

* Needs Attention* - Sources with:
- 1 file missing past expected window
- Volume variations 50-100% from normal
- Files arriving significantly late (>4 hours)
- Unexpected empty files that historically had data
- Schedule changes or timing anomalies

* No Action Needed* - Sources with:
- All files received and processed normally
- Volumes within acceptable ranges (±50%)
- No duplicates, failures, or timing issues
- Previous period uploads (informational only)

EXACT FORMATTING REQUIREMENTS:

1. **Start with Timestamp**: *Report generated at UTC HOUR*: HH:MM UTC
2. **Three Sections Only**: Urgent Action Required, Needs Attention, No Action Needed
3. **Bullet Format**: • *Source_Name (id: XXXXXX)* – YYYY-MM-DD: description
4. **Action Format**: → *Action:* specific steps (only for urgent/attention items)
5. **Record Counts**: `[X,XXX] records` for normal sources

DETAILED FORMATTING RULES:

**Urgent Action Required Section:**
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: X files missing past HH:MM–HH:MM UTC window — entities: [list entities] → *Action:* Notify provider to generate/re-send; re-run ingestion and verify completeness

**Needs Attention Section:**
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: [specific issue description] — [additional context] → *Action:* [if action needed]

**No Action Needed Section:**
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: `[X,XXX] records`
• All other recent files appear normal

SPECIFIC CONTENT EXTRACTION:

**Missing Files**: Extract specific file patterns, time windows, affected entities
**Duplicates/Failed**: Identify duplicate files, failed status, processing blocks
**Empty Files**: Note files that should have data but are empty
**Volume Variations**: Calculate percentage changes, identify unusual patterns
**Late Uploads**: Calculate delay times, compare to expected schedules
**Previous Period**: Note files from previous periods (usually informational)

ENTITY EXTRACTION:
From file names, extract business entities like: Clien_CBK, WhiteLabel, Shop, Google, POC, Market, Innovation, Donation, Beneficios, ApplePay, Anota-ai, AddCard, Clien_payments, ClienX_Clube, Saipos, etc.

TIME WINDOW ANALYSIS:
- Extract expected upload windows (e.g., 08:08–08:18 UTC)
- Calculate delays and early arrivals
- Note schedule changes or anomalies

VOLUME ANALYSIS:
- Calculate record counts for normal operations
- Identify volume increases/decreases with percentages
- Compare to historical baselines (e.g., "usual Monday 40k–55k")

ACTION RECOMMENDATIONS:
**For Missing Files**: "Notify provider to generate/re-send; re-run ingestion and verify completeness"
**For Schedule Changes**: "Confirm schedule change; adjust downstream triggers if needed"  
**For Volume Anomalies**: "Confirm coverage/window; monitor next run"
**For Late Files**: "Validate downstream completed; track if persists"

FINAL OUTPUT FORMAT - MUST MATCH EXACTLY:

*Report generated at UTC HOUR*: HH:MM UTC

* Urgent Action Required*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: [detailed issue] → *Action:* [specific steps]

* Needs Attention*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: [detailed issue] → *Action:* [specific steps if needed]

* No Action Needed*
• *Source_Name (id: XXXXXX)* – YYYY-MM-DD: `[X,XXX] records`
• All other recent files appear normal

CRITICAL FORMATTING RULES - FOLLOW EXACTLY:
- Use EXACT bullet format: • (bullet character, not dash or hyphen)
- Use EXACT bold formatting: *text* (asterisks, NOT **text** markdown)
- Use EXACT timestamp format: HH:MM UTC (24-hour format)
- Use EXACT record format: `[X,XXX] records` (backticks and brackets)
- Use EXACT action format: → *Action:* text (arrow character)
- Use EXACT section headers: * Urgent Action Required* (asterisk format)
- NO markdown formatting (**bold** is forbidden)
- NO additional sections beyond the three required
- NO executive summaries or additional analysis
- NO emojis, decorations, or extra formatting
- Keep descriptions specific with entities and time windows
- If no issues in a section, state "• No [issues/attention] required at this time."

EXAMPLE OF PERFECT FORMAT:
*Report generated at UTC HOUR*: 14:30 UTC

* Urgent Action Required*
• *Payments_Layout_1_V3 (id: 220504)* – 2025-09-07: 14 files missing past 08:08–08:18 UTC window — entities: Clien_CBK, WhiteLabel, Shop → *Action:* Notify provider to generate/re-send; re-run ingestion and verify completeness

* Needs Attention*  
• *Settlement_Layout_2 (id: 195385)* – 2025-09-08: Saipos file delivered early at 08:06 UTC (usual ~17:20) — Confirm schedule change

* No Action Needed*
• *Sale_payments_2 (id: 228036)* – 2025-09-08: `[1,233,496] records`
• All other recent files appear normal
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
