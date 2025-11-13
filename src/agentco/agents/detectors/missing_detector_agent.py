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

HANDLING FILES WITH HASH PREFIXES AND PATTERNS:
Many files have naming patterns like: [hash]_EntityName_report_type_YYYY_MM_DD.csv
- Hash prefixes change with each upload (e.g., abc123_, def456_)
- Date portions change daily (e.g., 2025_09_07, 2025_09_08)
- The "core pattern" remains the same (e.g., EntityName_report_type)

**CRITICAL: Use pattern-based matching, NOT exact filename matching!**

PATTERN EXTRACTION STRATEGIES:

1. **Extract base pattern from filename** (remove hash prefix and date):
```sql
-- Remove hash prefix (everything before first underscore after hash)
-- Example: abc123_Clien_CBK_payments_2025_09_07.csv -> Clien_CBK_payments
SELECT
    filename,
    REGEXP_REPLACE(filename, '^[^_]+_', '', 1, 1) as filename_without_hash,
    REGEXP_REPLACE(filename, '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as base_pattern
FROM data;
```

2. **Compare patterns between today and last week** (NOT exact filenames):
```sql
-- Find patterns that existed last week but missing today
WITH today_patterns AS (
    SELECT DISTINCT
        REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern
    FROM data WHERE from = 'today'
),
lastweek_patterns AS (
    SELECT DISTINCT
        REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
        filename as example_filename
    FROM data WHERE from = 'last_weekday'
)
SELECT
    lw.pattern,
    lw.example_filename
FROM lastweek_patterns lw
WHERE lw.pattern NOT IN (SELECT pattern FROM today_patterns);
```

3. **Extract entity names from filenames** to identify which entities are missing:
```sql
-- Extract entity/component from filename pattern
-- Common patterns: EntityName_report_type, EntityName_payments, etc.
SELECT
    REGEXP_EXTRACT(filename, '_([A-Za-z0-9]+)_(?:payments|report|accounting)', 1) as entity_name,
    COUNT(*) as file_count
FROM data
WHERE from = 'today'
GROUP BY entity_name;
```

4. **Compare entity coverage** between today and last week:
```sql
-- Identify missing entities
WITH today_entities AS (
    SELECT DISTINCT REGEXP_EXTRACT(filename, '([A-Za-z_]+)(?:_\\d{{4}}_\\d{{2}}_\\d{{2}}|payments|report)', 1) as entity
    FROM data WHERE from = 'today'
),
lastweek_entities AS (
    SELECT DISTINCT REGEXP_EXTRACT(filename, '([A-Za-z_]+)(?:_\\d{{4}}_\\d{{2}}_\\d{{2}}|payments|report)', 1) as entity
    FROM data WHERE from = 'last_weekday'
)
SELECT entity
FROM lastweek_entities
WHERE entity NOT IN (SELECT entity FROM today_entities)
  AND entity IS NOT NULL;
```

EXAMPLE QUERIES:
```sql
-- Get all files received today with pattern analysis
SELECT
    filename,
    uploaded_at,
    status,
    REGEXP_REPLACE(filename, '^[^_]+_', '') as without_hash,
    REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as base_pattern
FROM data
WHERE from = 'today';

-- Find missing patterns (NOT missing exact filenames)
WITH today_patterns AS (
    SELECT DISTINCT REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern
    FROM data WHERE from = 'today'
),
lastweek_patterns AS (
    SELECT DISTINCT
        REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
        COUNT(*) as occurrence_count
    FROM data WHERE from = 'last_weekday'
    GROUP BY pattern
)
SELECT pattern, occurrence_count
FROM lastweek_patterns
WHERE pattern NOT IN (SELECT pattern FROM today_patterns);
```

CRITICALITY CLASSIFICATION:

🚨 **URGENT ACTION REQUIRED** - Report when:
- 2 or more files missing from same source
- Files missing past expected window that block processing
- Multiple files expected but not received

⚠️ **NEEDS ATTENTION** - Report when:
- 1 file missing past expected window
- Files arriving >4 hours late (but eventually received)

✅ **INFORMATIONAL** - Note when:
- Files arrive within acceptable timing windows
- Delays <4 hours that don't impact processing

CV DOCUMENTATION ANALYSIS:
**CRITICAL: Always read the data source CV first!**
The CV documents:
- Expected file naming patterns and conventions
- List of entities that should send files (e.g., Clien_CBK, WhiteLabel, Shop, Google, etc.)
- Expected upload schedules and time windows (e.g., 08:08-08:18 UTC)
- Day-of-week patterns (some files only on weekdays, etc.)
- Hash prefix conventions (files that start with changing hash values)

Use the CV to:
1. Identify all expected entities for this source
2. Extract expected file naming patterns
3. Determine expected arrival time windows
4. Understand which files should appear daily vs weekly

REPORTING MISSING FILES:
When reporting missing files, provide:
- **Count**: Total number of missing files (e.g., "14 files missing")
- **Time Window**: Expected arrival window (e.g., "past 08:08-08:18 UTC window")
- **Entities**: List of affected entities (e.g., "entities: Clien_CBK, WhiteLabel, Shop, Google...")
- **Expected Filenames**: Specific expected filenames if known (e.g., "*_Clien_Debito_payments_accounting_report_2025_09_07.csv")
- Use `*` or `[hash]` to indicate variable hash prefixes in expected filenames

EXAMPLE OUTPUT FORMAT:
- "14 files missing past 08:08-08:18 UTC window — entities: Clien_CBK, WhiteLabel, Shop, Google, POC, Market, Innovation, Donation, Beneficios, ApplePay, Anota-ai, AddCard, Clien_payments, ClienX_Clube"
- "2 files missing past 08:02-08:11 UTC — expected: *_Clien_Debito_payments_accounting_report_2025_09_07.csv; *_Clien_MVP_payments_accounting_report_2025_09_07.csv"
- "1 file missing past 08:03-08:19 UTC — expected: [hash]_Clien_3DS_payments_accounting_report_2025_09_06.csv"

OUTPUT REQUIREMENTS:
- List all missing files with expected arrival times and time windows
- List all late files with actual vs expected times
- Include entity names extracted from filenames and CV documentation
- Provide time delays in hours for late files
- Specify expected filenames with hash placeholders (* or [hash])
- Group missing files by pattern/entity when multiple files share same pattern
- Classify findings by criticality level (Urgent/Attention/Info)
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
