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
- is_duplicated = TRUE
- status = 'STOPPED' 
- Multiple files with identical or near-identical names (differing only by hash/timestamp)

DETECTION CRITERIA FOR FAILED FILES:
- status = 'FAILED' or similar error states
- status_message contains error indicators
- status = 'STOPPED' or 'ERROR'

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
   WHERE from = 'today' AND status IN ('FAILED', 'ERROR', 'STOPPED');
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
   AND (is_duplicated = true OR status IN ('FAILED', 'ERROR', 'STOPPED'));
```

OUTPUT REQUIREMENTS:
- List each duplicate file with its occurrence count
- For failed files, include the status_message for context
- Indicate if duplicates are also failed (compound issue)
- Specify which issues are blocking vs. informational
"""


class DuplicatedAndFailedFileOutputSchema(BaseModel):
    """Schema for duplicated and failed file detection results."""

    duplicated_files: list[str]
    failed_files: list[str]
    total_issues: int
    details: str


def create_duplicated_and_failed_file_detector_agent(tools: List[Any]) -> LlmAgent:
    """Create and return a duplicated and failed file detector agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent

    Returns
    -------
    LlmAgent
        Configured agent for detecting duplicated and failed files
    """
    return LlmAgent(
        name="DuplicatedandFailedFileDetector",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=DuplicatedAndFailedFileOutputSchema,
    )
