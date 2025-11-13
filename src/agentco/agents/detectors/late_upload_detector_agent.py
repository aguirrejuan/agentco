"""Late Upload Detector Agent

This module contains the agent responsible for detecting files uploaded significantly later than their expected schedule.
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

MISSION: Detect files uploaded significantly later than their expected schedule.

SEVERITY: This produces WARNING-level incidents, NOT urgent issues.

ANALYSIS STEPS:
1. Read data source CV to get expected arrival times for each file

2. Get today's upload times:
```sql
   SELECT filename, uploaded_at, status 
   FROM data 
   WHERE from = 'today'
   ORDER BY uploaded_at;
```

3. Compare with typical arrival times:
```sql
   SELECT 
       t.filename,
       t.uploaded_at as today_time,
       l.uploaded_at as lastweek_time,
       ROUND(EXTRACT(EPOCH FROM (t.uploaded_at::timestamp - l.uploaded_at::timestamp))/3600, 2) as hour_difference
   FROM 
       (SELECT * FROM data WHERE from = 'today') t
   INNER JOIN 
       (SELECT * FROM data WHERE from = 'last_weekday') l
   ON t.filename = l.filename;
```

4. Identify significantly late files:
```sql
   SELECT 
       t.filename,
       t.uploaded_at as today_time,
       l.uploaded_at as lastweek_time,
       ROUND(EXTRACT(EPOCH FROM (t.uploaded_at::timestamp - l.uploaded_at::timestamp))/3600, 2) as hour_difference
   FROM 
       (SELECT * FROM data WHERE from = 'today') t
   INNER JOIN 
       (SELECT * FROM data WHERE from = 'last_weekday') l
   ON t.filename = l.filename
   WHERE EXTRACT(EPOCH FROM (t.uploaded_at::timestamp - l.uploaded_at::timestamp))/3600 > 4;
```

DETECTION CRITERIA:
- File arrived >4 hours later than expected schedule window
- Significant delay compared to historical same-weekday arrivals
- Consider timezone differences documented in CV
- Compare upload time patterns from CV

IMPORTANT:
- This is a WARNING, not a critical error
- Files that eventually arrive are less critical than missing files
- Focus on delays that could impact downstream processing
- Note if early arrivals occur (may indicate schedule changes)

DO NOT FLAG:
- Files with <4 hour delays
- Files documented as having variable arrival times
- Manual/historical uploads (these are expected to be off-schedule)
- Files arriving early (note separately as informational)

OUTPUT REQUIREMENTS:
- Files with >4 hour delay, sorted by severity of delay
- Include actual upload time vs. expected time
- Calculate delay in hours
- Note if this is consistently late or a one-time issue
"""


class FileUploadAfterScheduleOutputSchema(BaseModel):
    """Schema for late file upload detection results."""

    files_uploaded_after_schedule: list[str]
    total_files_uploaded_after_schedule: int
    details: str


def create_file_upload_after_schedule_detector_agent(tools: List[Any]) -> LlmAgent:
    """Create and return a late file upload detector agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent

    Returns
    -------
    LlmAgent
        Configured agent for detecting files uploaded after their expected schedule
    """
    return LlmAgent(
        name="FileUploadAfterScheduleDetector",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=FileUploadAfterScheduleOutputSchema,
        output_key="late_upload_results",  # Store results in session state
    )
