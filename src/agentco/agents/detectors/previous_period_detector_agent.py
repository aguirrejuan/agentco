"""Previous Period File Upload Detector Agent

This module contains the agent responsible for identifying files from previous periods uploaded outside their expected time windows.
"""

from typing import Any, List

from google.adk.agents import LlmAgent
from google.adk.planners import BuiltInPlanner
from google.genai.types import ThinkingConfig
from pydantic import BaseModel

from ..commons import COMMON_INSTRUCTIONS, get_model

# Thinking configuration
thinking_config = ThinkingConfig(include_thoughts=True, thinking_budget=256)

# Instantiate BuiltInPlanner
planner = BuiltInPlanner(thinking_config=thinking_config)


PROMPT_TEMPLATE = """
{COMMON_INSTRUCTIONS}

MISSION: Identify files from previous periods uploaded outside their expected time windows.

CONTEXT: These are typically historical backfills or manual uploads, NOT critical errors.

ANALYSIS STEPS:
1. Read data source CV to understand:
   - ECD (Expected Coverage Data) windows
   - Normal vs. manual upload patterns
   - File naming patterns that indicate date/period

2. Identify period from filename (common patterns):
```sql
   SELECT 
       filename,
       uploaded_at,
       status,
       -- Try to extract date from filename (multiple patterns)
       COALESCE(
           REGEXP_EXTRACT(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1),
           REGEXP_EXTRACT(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1),
           REGEXP_EXTRACT(filename, '(\\d{{8}})', 1)
       ) as file_date_str
   FROM data
   WHERE from = 'today';
```

3. Detect date mismatches:
```sql
   SELECT 
       filename,
       uploaded_at,
       REGEXP_EXTRACT(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) as file_date
   FROM data
   WHERE from = 'today'
     AND REGEXP_EXTRACT(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) IS NOT NULL
     AND REGEXP_EXTRACT(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) != STRFTIME(CURRENT_DATE, '%Y_%m_%d')
     AND REGEXP_EXTRACT(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) < STRFTIME(CURRENT_DATE, '%Y_%m_%d');
```

4. Compare with expected coverage dates:
```sql
   SELECT 
       filename,
       uploaded_at,
       REGEXP_EXTRACT(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) as file_date,
       DATEDIFF('day', 
           CAST(REGEXP_EXTRACT(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) AS DATE),
           CURRENT_DATE
       ) as days_old
   FROM data
   WHERE from = 'today'
     AND REGEXP_EXTRACT(filename, '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) IS NOT NULL;
```

DETECTION CRITERIA:
- File date in filename doesn't match upload date
- File uploaded outside its ECD window
- File represents data from previous days/weeks/months
- Indicates historical/backfill/manual processing

CLASSIFICATION:
- ⚠️ INFORMATIONAL: Not a critical error
- These uploads are often intentional corrections
- Should be tracked but not flagged as urgent
- May indicate system recovery or data reconciliation

OUTPUT REQUIREMENTS:
- List files with period mismatch
- Include how many days old the data is
- Note they are informational/manual uploads
- Distinguish between recent backfills (1-2 days) vs. historical (>7 days)
"""


class UploadOfPreviousFileOutputSchema(BaseModel):
    """Schema for previous period file upload detection results."""

    previous_period_files: list[str]
    total_previous_period_files: int
    details: str


def create_upload_of_previous_file_detector_agent(
    tools: List[Any], source_id: str = None
) -> LlmAgent:
    """Create and return an upload of previous file detector agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent
    source_id : str, optional
        Source identifier for unique output key generation

    Returns
    -------
    LlmAgent
        Configured agent for detecting upload of previous files
    """
    # Generate source-specific output key
    output_key = (
        f"previous_period_results_{source_id}"
        if source_id
        else "previous_period_results"
    )

    return LlmAgent(
        name="UploadOfPreviousFileDetector",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=UploadOfPreviousFileOutputSchema,
        output_key=output_key,  # Store results in session state with unique key
    )
