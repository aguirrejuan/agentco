"""Duplicated and Failed File Detector Agent

This module contains the agent responsible for detecting duplicate files and files with processing errors.
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

MISSION: Identify duplicate files and files with processing errors.

DETECTION CRITERIA FOR DUPLICATES:
- is_duplicated = TRUE AND status = 'stopped'
- Multiple files with identical or near-identical names (differing only by hash/timestamp)
- Duplicate files are typically blocked from processing (status = 'stopped')

DETECTION CRITERIA FOR FAILED FILES:
- status = 'failure' (processing failed)
- status = 'stopped' (processing was stopped/blocked)
- status = 'deleted' (file was deleted, possibly due to errors)
- status_message contains error indicators

STATUS REFERENCE:
- 'processed' = successfully processed
- 'stopped' = processing stopped/blocked (often duplicates)
- 'empty' = processed but contains no data
- 'failure' = processing failed with errors
- 'deleted' = file was removed from system

ANALYSIS STEPS:
1. Query for duplicates: 
```sql
   SELECT filename, COUNT(*) as count, uploaded_at, status 
   FROM data 
   WHERE from = 'today' AND is_duplicated = true
   GROUP BY filename, uploaded_at, status;
```

2. Query for failed files:
```sql
   SELECT filename, status, status_message, uploaded_at
   FROM data
   WHERE from = 'today' AND status IN ('failure', 'stopped', 'deleted');
```

3. Identify naming pattern duplicates:
```sql
   SELECT 
       REGEXP_REPLACE(filename, '^[^_]+_', '') as base_name,
       COUNT(*) as count,
       STRING_AGG(filename, ', ') as all_versions
   FROM data 
   WHERE from = 'today'
   GROUP BY base_name
   HAVING COUNT(*) > 1;
```

4. Cross-check duplicates that are also failed:
```sql
   SELECT filename, is_duplicated, status, status_message
   FROM data
   WHERE from = 'today'
   AND (is_duplicated = true OR status IN ('failure', 'stopped', 'deleted'));
```

CRITICALITY CLASSIFICATION:

🚨 **URGENT ACTION REQUIRED** - Report when:
- Critical duplicated files blocking processing (status = 'stopped')
- Multiple files failed with same error pattern
- Files with status = 'failure' that prevent downstream processing
- Any processing-blocking issues (duplicates + failures combined)

⚠️ **NEEDS ATTENTION** - Report when:
- Individual duplicate files (status = 'stopped') that need resolution
- Failed files (status = 'failure') with non-critical errors
- Files marked as 'deleted' that may need investigation

✅ **INFORMATIONAL** - Note when:
- All files processed successfully (status = 'processed')
- No duplicates or failures detected

OUTPUT REQUIREMENTS:
- List each duplicate file with its occurrence count
- For failed files, include the status_message for context
- Indicate if duplicates are also failed (compound issue)
- Classify findings by criticality level (Urgent/Attention/Info)
- Specify which issues are blocking vs. informational
"""


class DuplicatedAndFailedFileOutputSchema(BaseModel):
    """Schema for duplicated and failed file detection results."""

    source_id: str
    source_name: str
    duplicated_files: list[str]
    failed_files: list[str]
    total_issues: int
    details: str


def create_duplicated_and_failed_file_detector_agent(
    tools: List[Any], source_id: str = None
) -> LlmAgent:
    """Create and return a duplicated and failed file detector agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent
    source_id : str, optional
        Source identifier for unique output key generation

    Returns
    -------
    LlmAgent
        Configured agent for detecting duplicated and failed files
    """
    # Generate source-specific output key
    output_key = (
        f"duplicated_failed_results_{source_id}"
        if source_id
        else "duplicated_failed_results"
    )

    return LlmAgent(
        name="DuplicatedandFailedFileDetector",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=DuplicatedAndFailedFileOutputSchema,
        output_key=output_key,  # Store results in session state with unique key
    )
