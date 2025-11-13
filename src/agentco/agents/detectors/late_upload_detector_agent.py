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

STATUS CONSIDERATIONS FOR LATE UPLOADS:
- Check uploaded_at time for ALL statuses (late uploads can have any status)
- Include files with any status: 'processed', 'empty', 'failure', 'stopped'
- Status indicates processing outcome, not upload timing
- Even failed files can be flagged as late uploads

STATUS REFERENCE:
- 'processed' = successfully processed
- 'stopped' = processing stopped/blocked
- 'empty' = processed but contains no data
- 'failure' = processing failed with errors
- 'deleted' = file was removed from system

ANALYSIS STEPS:
1. Read data source CV to get expected arrival times for each file

2. Get today's upload times:
```sql
   SELECT filename, uploaded_at, status 
   FROM data 
   WHERE from = 'today'
   ORDER BY uploaded_at;
```

3. **CRITICAL: Use PATTERN-based matching for files with hash prefixes:**
```sql
   -- Compare upload times by PATTERN (not exact filename)
   WITH today_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           REGEXP_EXTRACT(filename, '([A-Za-z0-9_]+)(?:_\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) as entity_name,
           filename,
           uploaded_at as today_time
       FROM data
       WHERE from = 'today'
   ),
   lastweek_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           uploaded_at as lastweek_time
       FROM data
       WHERE from = 'last_weekday'
   )
   SELECT
       t.entity_name,
       t.filename,
       t.today_time,
       l.lastweek_time,
       ROUND(EXTRACT(EPOCH FROM (t.today_time - l.lastweek_time))/3600, 2) as hour_difference
   FROM today_data t
   INNER JOIN lastweek_data l ON t.pattern = l.pattern;
```

4. Identify LATE files (>4 hours after expected):
```sql
   WITH today_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           REGEXP_EXTRACT(filename, '([A-Za-z0-9_]+)(?:_\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) as entity_name,
           filename,
           uploaded_at as today_time
       FROM data
       WHERE from = 'today'
   ),
   lastweek_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           uploaded_at as lastweek_time
       FROM data
       WHERE from = 'last_weekday'
   )
   SELECT
       t.entity_name,
       t.filename,
       t.today_time,
       l.lastweek_time,
       ROUND(EXTRACT(EPOCH FROM (t.today_time - l.lastweek_time))/3600, 2) as hour_difference
   FROM today_data t
   INNER JOIN lastweek_data l ON t.pattern = l.pattern
   WHERE EXTRACT(EPOCH FROM (t.today_time - l.lastweek_time))/3600 > 4;
```

5. Identify EARLY files (arriving significantly earlier than expected):
```sql
   WITH today_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           REGEXP_EXTRACT(filename, '([A-Za-z0-9_]+)(?:_\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) as entity_name,
           filename,
           uploaded_at as today_time
       FROM data
       WHERE from = 'today'
   ),
   lastweek_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           uploaded_at as lastweek_time
       FROM data
       WHERE from = 'last_weekday'
   )
   SELECT
       t.entity_name,
       t.filename,
       t.today_time,
       l.lastweek_time,
       ROUND(EXTRACT(EPOCH FROM (l.lastweek_time - t.today_time))/3600, 2) as hours_early
   FROM today_data t
   INNER JOIN lastweek_data l ON t.pattern = l.pattern
   WHERE EXTRACT(EPOCH FROM (l.lastweek_time - t.today_time))/3600 > 4;
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

CRITICALITY CLASSIFICATION:

🚨 **URGENT ACTION REQUIRED** - Report when:
- Multiple files arriving significantly late (>8 hours delay)
- Late files that cause downstream processing failures
- Critical time-sensitive files missing their processing windows

⚠️ **NEEDS ATTENTION** - Report when:
- Files arriving >4 hours later than expected schedule window
- Significant delay compared to historical same-weekday arrivals
- Schedule changes or timing anomalies that need confirmation
- Late arrivals that may impact downstream processing

✅ **INFORMATIONAL** - Note when:
- Files arriving within expected timing windows (<4 hour delay)
- Files arriving early (schedule changes to monitor)
- Delays that don't impact downstream processing

CV SCHEDULE ANALYSIS:
**CRITICAL: Always read the data source CV first!**
The CV documents expected arrival schedules, such as:
- Expected time windows (e.g., "08:08-08:18 UTC")
- Typical arrival times (e.g., "usual ~17:20 UTC")
- Day-of-week patterns (weekdays vs weekends)
- Timezone specifications

Use the CV to:
1. Identify expected arrival times for each file/entity
2. Calculate delays relative to documented schedules (not just last week)
3. Distinguish between:
   - Files arriving within expected window (normal)
   - Files arriving late (>4 hours past expected window)
   - Files arriving early (significantly before expected time - may indicate schedule change)

ENTITY EXTRACTION:
Extract entity names from filenames to provide specific reporting:
- Saipos, ClienX, Clien_CBK, WhiteLabel, Shop, Google, etc.
- Report as: "EntityName file delivered..."

REPORTING FORMAT EXAMPLES:

**For Early Arrivals (INFORMATIONAL):**
- "Saipos file delivered early at 08:06 UTC (usual ~17:20) — Confirm schedule change; adjust downstream triggers if needed"
- "ClienX arrived 9 hours early at 06:15 UTC (expected ~15:00)"

**For Late Arrivals (NEEDS ATTENTION):**
- "Saipos file arrived 4.2 hours late at 12:20 UTC (expected 08:08 UTC)"
- "ClienX delayed 6.5 hours, uploaded at 14:38 UTC (expected 08:08-08:18 UTC window)"

**Note Schedule Change Implications:**
- Early arrivals: "Confirm schedule change; adjust downstream triggers if needed"
- Late arrivals: "Validate downstream completed; track if persists"
- Persistent timing changes: "Confirm intentional lag change; keep a short-term volume watch"

OUTPUT REQUIREMENTS:
- Files with >4 hour delay, sorted by severity of delay
- Files arriving significantly early (>4 hours early) - note as schedule change
- Extract and include entity names (e.g., "Saipos file delivered...")
- Include actual upload time vs. expected time (from CV or historical)
- Calculate delay/early arrival in hours
- Reference expected time windows from CV documentation
- Suggest actions based on timing change type
- Classify findings by criticality level (Urgent/Attention/Info)
- Note if this is consistently late or a one-time issue
"""


class FileUploadAfterScheduleOutputSchema(BaseModel):
    """Schema for late file upload detection results."""

    source_id: str
    source_name: str
    files_uploaded_after_schedule: list[str]
    total_files_uploaded_after_schedule: int
    details: str


def create_file_upload_after_schedule_detector_agent(
    tools: List[Any], source_id: str = None
) -> LlmAgent:
    """Create and return a file upload after schedule detector agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent
    source_id : str, optional
        Source identifier for unique output key generation

    Returns
    -------
    LlmAgent
        Configured agent for detecting files uploaded after schedule
    """
    # Generate source-specific output key
    output_key = (
        f"late_upload_results_{source_id}" if source_id else "late_upload_results"
    )

    return LlmAgent(
        name="FileUploadAfterScheduleDetector",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=FileUploadAfterScheduleOutputSchema,
        output_key=output_key,  # Store results in session state with unique key
    )
