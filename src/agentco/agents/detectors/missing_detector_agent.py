"""Missing File Detector Agent

This module contains the agent responsible for identifying files that were expected but not received,
or arrived significantly late.
"""

from typing import Any, List

from google.adk.agents import LlmAgent
from google.adk.planners import BuiltInPlanner
from google.genai.types import ThinkingConfig
from pydantic import BaseModel

from ...logger import logger
from ..commons import COMMON_INSTRUCTIONS, get_model

# Thinking configuration
thinking_config = ThinkingConfig(include_thoughts=True, thinking_budget=256)

# Instantiate BuiltInPlanner
planner = BuiltInPlanner(thinking_config=thinking_config)


PROMPT_TEMPLATE = """
{COMMON_INSTRUCTIONS}

MISSION: Identify files that were expected but not received, or arrived significantly late.

DETECTION CRITERIA:
- Files documented in the data source CV that are missing from today's data
- Files that arrived >4 hours after their expected schedule window
- Consider day-of-week patterns (e.g., some files may not be expected on weekends)

STATUS CONSIDERATIONS:
- Missing files = Files expected but NOT in today's data (any status)
- Check all statuses when identifying received files
- A file with status = 'deleted' may indicate it was received then removed
- Focus on completely missing files vs files with error statuses

STATUS REFERENCE:
- 'processed' = successfully processed
- 'stopped' = processing stopped/blocked
- 'empty' = processed but contains no data
- 'failure' = processing failed with errors
- 'deleted' = file was removed from system

ANALYSIS STEPS:
1. Read data source CV to identify all expected files and their schedules
2. Query today's data: SELECT DISTINCT filename, uploaded_at, status FROM data WHERE from = 'today'
3. Compare expected vs. actual files received
4. For late files, calculate delay: uploaded_at - expected_time
5. Check last_weekday data to confirm if this is a recurring pattern or new issue

IMPORTANT CONSIDERATIONS:
- A file missing on a specific day may be normal (check CV for schedule patterns)
- Only flag files that are SIGNIFICANTLY late (>4 hours past expected window)
- Distinguish between "missing" (not received at all) and "late" (received but delayed)

EXAMPLE QUERIES:
```sql
-- Get all files received today
SELECT filename, uploaded_at, status FROM data WHERE from = 'today';

-- Compare today vs last week same day
SELECT 
    today.filename,
    today.uploaded_at as today_time,
    lastweek.uploaded_at as lastweek_time
FROM 
    (SELECT * FROM data WHERE from = 'today') today
FULL OUTER JOIN 
    (SELECT * FROM data WHERE from = 'last_weekday') lastweek
ON today.filename = lastweek.filename;

-- Find files in last week but not today
SELECT filename 
FROM data 
WHERE from = 'last_weekday' 
AND filename NOT IN (SELECT filename FROM data WHERE from = 'today');
```

OUTPUT REQUIREMENTS:
- List all missing files with expected arrival times
- List all late files with actual vs expected times
- Include entity names from filenames where applicable
- Provide time delays in hours for late files
"""


class MissingFileOutputSchema(BaseModel):
    """Schema for missing file detection results."""

    source_id: str
    source_name: str
    missing_files: list[str]
    missing_files_number: int
    late_files: list[str]
    late_files_number: int
    details: str


def create_missing_file_detector_agent(
    tools: List[Any], source_id: str = None
) -> LlmAgent:
    """Create and return a missing file detector agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent
    source_id : str, optional
        Source identifier for unique output key generation

    Returns
    -------
    LlmAgent
        Configured agent for detecting missing and late files
    """
    # Generate source-specific output key
    output_key = (
        f"missing_file_results_{source_id}" if source_id else "missing_file_results"
    )

    logger.debug(
        f"Creating MissingFileDetector agent for source_id={source_id} with output_key={output_key}"
    )

    return LlmAgent(
        name="MissingFileDetector",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=MissingFileOutputSchema,
        output_key=output_key,  # Store results in session state with unique key
    )
