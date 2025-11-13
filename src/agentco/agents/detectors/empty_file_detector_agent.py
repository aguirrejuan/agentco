"""Unexpected Empty File Detector Agent

This module contains the agent responsible for detecting files that are unexpectedly empty.
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

MISSION: Identify files that are unexpectedly empty (0 records) when they should contain data.

CRITICAL: Not all empty files are issues!
- Check data source CV for files that are NORMALLY empty
- Only flag files that have a PATTERN of containing data but are now empty

KEY STATUS VALUES FOR EMPTY FILES:
- 'empty' = file marked as empty (0 records)
- 'processed' = successfully processed (may still have 0 records)
- 'failure' = may have failed due to being empty
- Focus on files where rows = 0 regardless of status

STATUS REFERENCE:
- 'processed' = successfully processed
- 'stopped' = processing stopped/blocked
- 'empty' = processed but contains no data
- 'failure' = processing failed with errors
- 'deleted' = file was removed from system

ANALYSIS STEPS:
1. Read data source CV to identify which files should typically contain data

2. Find empty files today:
```sql
   SELECT filename, rows, uploaded_at 
   FROM data 
   WHERE from = 'today' AND rows = 0;
```

3. Compare with historical patterns:
```sql
   SELECT 
       t.filename,
       t.rows as today_rows,
       l.rows as lastweek_rows
   FROM 
       (SELECT * FROM data WHERE from = 'today' AND rows = 0) t
   LEFT JOIN 
       (SELECT * FROM data WHERE from = 'last_weekday') l
   ON t.filename = l.filename;
```

4. Calculate empty file frequency pattern:
```sql
   SELECT 
       l.filename,
       l.rows as lastweek_rows,
       t.rows as today_rows
   FROM 
       (SELECT * FROM data WHERE from = 'last_weekday') l
   LEFT JOIN
       (SELECT * FROM data WHERE from = 'today') t
   ON l.filename = t.filename
   WHERE l.rows > 10 AND (t.rows = 0 OR t.rows IS NULL);
```

DECISION LOGIC:
- ✅ FLAG: File normally has >0 rows but is empty today
- ✅ FLAG: File had data last week but is empty this week
- ❌ DON'T FLAG: File is documented as regularly empty
- ❌ DON'T FLAG: File has consistently been empty historically
- ❌ DON'T FLAG: Empty files that are expected based on CV documentation

CRITICALITY CLASSIFICATION:

🚨 **URGENT ACTION REQUIRED** - Report when:
- Multiple files unexpectedly empty that historically contain significant data
- Empty files that block downstream processing or reporting
- Critical data sources showing 0 records when pattern shows >1000 records typically

⚠️ **NEEDS ATTENTION** - Report when:
- Single file unexpectedly empty that historically had data
- Files with rows = 0 when last week had >10 records
- Unexpected empty files that don't match documented patterns

✅ **INFORMATIONAL** - Note when:
- Empty files (rows = 0) that are documented as regularly empty
- Files with status = 'empty' that match historical patterns
- Files consistently empty across historical data

OUTPUT FORMAT:
Include only files where empty state is UNEXPECTED based on historical patterns.
Provide comparison with last week's row count for context.
Classify findings by criticality level (Urgent/Attention/Info).
"""


class UnexpectedEmptyFileOutputSchema(BaseModel):
    """Schema for unexpected empty file detection results."""

    source_id: str
    source_name: str
    empty_files: list[str]
    total_empty_files: int
    details: str


def create_unexpected_empty_file_detector_agent(
    tools: List[Any], source_id: str = None
) -> LlmAgent:
    """Create and return an unexpected empty file detector agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent
    source_id : str, optional
        Source identifier for unique output key generation

    Returns
    -------
    LlmAgent
        Configured agent for detecting unexpected empty files
    """
    # Generate source-specific output key
    output_key = (
        f"empty_file_results_{source_id}" if source_id else "empty_file_results"
    )

    return LlmAgent(
        name="UnexpectedEmptyFileDetector",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=UnexpectedEmptyFileOutputSchema,
        output_key=output_key,  # Store results in session state with unique key
    )
