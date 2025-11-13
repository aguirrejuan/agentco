"""Volume Variation Detector Agent

This module contains the agent responsible for detecting anomalous volume variations based on day-of-week patterns.
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

MISSION: Detect anomalous volume variations based on day-of-week patterns.

KEY PRINCIPLE: Compare like-to-like (Monday to Monday, Tuesday to Tuesday, etc.)

STATUS CONSIDERATIONS FOR VOLUME ANALYSIS:
- Focus on files with status = 'processed' or 'empty' for fair comparison
- Exclude 'failure', 'stopped', 'deleted' from volume comparisons (incomplete processing)
- Volume variations should compare successfully processed files only
- An 'empty' status file still has rows = 0, which is valid for comparison

STATUS REFERENCE:
- 'processed' = successfully processed (use for volume comparison)
- 'empty' = processed but contains no data (use for volume comparison)
- 'stopped' = processing stopped/blocked (exclude from volume comparison)
- 'failure' = processing failed with errors (exclude from volume comparison)
- 'deleted' = file was removed from system (exclude from volume comparison)

ANALYSIS STEPS:
1. Read data source CV to understand normal volume ranges and patterns

2. Get today's volumes (only successfully processed files):
```sql
   SELECT filename, rows, uploaded_at, status
   FROM data
   WHERE from = 'today'
     AND status IN ('processed', 'empty')
   ORDER BY rows DESC;
```

3. **CRITICAL: Use PATTERN-based matching, NOT exact filename matching!**
   Files with hash prefixes need pattern extraction for comparison:
```sql
   -- Compare volumes by PATTERN (not exact filename) to handle hash prefixes
   WITH today_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           REGEXP_EXTRACT(filename, '([A-Za-z0-9_]+)(?:_\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) as entity_name,
           filename,
           rows as today_volume
       FROM data
       WHERE from = 'today' AND status IN ('processed', 'empty')
   ),
   lastweek_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           REGEXP_EXTRACT(filename, '([A-Za-z0-9_]+)(?:_\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) as entity_name,
           filename,
           rows as lastweek_volume
       FROM data
       WHERE from = 'last_weekday' AND status IN ('processed', 'empty')
   )
   SELECT
       t.filename as today_filename,
       t.entity_name,
       t.pattern,
       t.today_volume,
       l.lastweek_volume,
       ROUND(((t.today_volume - l.lastweek_volume) * 100.0 / NULLIF(l.lastweek_volume, 0)), 2) as pct_change,
       ABS(t.today_volume - l.lastweek_volume) as abs_difference
   FROM today_data t
   INNER JOIN lastweek_data l ON t.pattern = l.pattern
   WHERE l.lastweek_volume > 0;
```

4. Identify significant variations (>50% change) with entity extraction:
```sql
   WITH today_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           REGEXP_EXTRACT(filename, '([A-Za-z0-9_]+)(?:_\\d{{4}}_\\d{{2}}_\\d{{2}})', 1) as entity_name,
           filename,
           rows as today_volume
       FROM data
       WHERE from = 'today' AND status IN ('processed', 'empty')
   ),
   lastweek_data AS (
       SELECT
           REGEXP_REPLACE(REGEXP_REPLACE(filename, '^[^_]+_', ''), '_\\d{{4}}_\\d{{2}}_\\d{{2}}', '') as pattern,
           rows as lastweek_volume
       FROM data
       WHERE from = 'last_weekday' AND status IN ('processed', 'empty')
   )
   SELECT
       t.entity_name,
       t.today_volume,
       l.lastweek_volume,
       ROUND(((t.today_volume - l.lastweek_volume) * 100.0 / NULLIF(l.lastweek_volume, 0)), 2) as pct_change
   FROM today_data t
   INNER JOIN lastweek_data l ON t.pattern = l.pattern
   WHERE l.lastweek_volume > 0
     AND ABS(((t.today_volume - l.lastweek_volume) * 100.0 / NULLIF(l.lastweek_volume, 0))) > 50;
```

ANOMALY DETECTION RULES:
- Volume change >50% compared to same weekday last week
- Sudden spikes or drops that deviate from documented patterns
- Volume significantly different from expected ranges in CV

SPECIAL CONSIDERATIONS:
- Weekend patterns may differ from weekdays (verify in CV)
- End-of-month files may have different volumes
- Seasonal patterns (check CV for seasonal expectations)
- Check if variation is consistent with business events

THRESHOLDS:
- 🚨 CRITICAL: >100% increase or >80% decrease
- ⚠️ WARNING: 50-100% increase or 50-80% decrease
- ✅ NORMAL: <50% variation

CRITICALITY CLASSIFICATION:

🚨 **URGENT ACTION REQUIRED** - Report when:
- Volume changes >100% increase from normal baseline
- Volume changes >80% decrease from normal baseline
- Multiple files showing critical variations (processing anomaly)
- Significant volume deviations outside documented ranges

⚠️ **NEEDS ATTENTION** - Report when:
- Volume variations 50-100% increase from same weekday last week
- Volume variations 50-80% decrease from same weekday last week
- Single files with significant but explainable variations
- Volumes outside expected ranges but within business tolerance

✅ **INFORMATIONAL** - Note when:
- Volumes within ±50% of normal baseline
- Variations within documented acceptable ranges
- Expected volume patterns (weekend, end-of-month, etc.)

CV VOLUME RANGE ANALYSIS:
**CRITICAL: Always read the data source CV first!**
The CV documents expected volume ranges, such as:
- "usual Monday 40k-55k records"
- "typical range 800k-900k"
- "95% confidence band: 50,211-869,600"
- "expected volume >1000 records"

Extract these ranges from the CV and use them to contextualize variations:
- Compare actual volume to documented expected ranges
- Note if volume exceeds confidence bands or typical bounds
- Reference day-of-week patterns (Monday volumes vs Tuesday, etc.)

ENTITY EXTRACTION:
Extract entity names from filenames to provide specific reporting:
- ClienX, Saipos, Clien_CBK, WhiteLabel, Shop, Google, etc.
- Report as: "EntityName volume X (comparison to baseline)"

REPORTING FORMAT EXAMPLES:
- "ClienX volume 61,639 (> usual Monday 40k–55k)"
- "ClienX volume 56,277 (>95% bound 50,211)"
- "ClienX 1,023,337 (>95% band 869,600)"
- "Volume decreased 75% from last week (5,000 vs 20,000)"

OUTPUT REQUIREMENTS:
- Only files with SIGNIFICANT unexpected variations (>50% change)
- Extract and include entity names in reports (e.g., "ClienX volume...")
- Include both today's volume and comparison baseline
- Reference CV-documented expected ranges when available (e.g., "usual Monday 40k-55k")
- Calculate and report percentage change when not using CV ranges
- Note if volume exceeds confidence bands or statistical bounds
- Classify findings by criticality level (Urgent/Attention/Info)
- Consider documented volume ranges from CV
"""


class UnexpectedVolumeVariationOutputSchema(BaseModel):
    """Schema for unexpected volume variation detection results."""

    source_id: str
    source_name: str
    unexpected_volume_files: list[str]
    total_unexpected_volume_files: int
    details: str


def create_unexpected_volume_variation_detector_agent(
    tools: List[Any], source_id: str = None
) -> LlmAgent:
    """Create and return an unexpected volume variation detector agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent
    source_id : str, optional
        Source identifier for unique output key generation

    Returns
    -------
    LlmAgent
        Configured agent for detecting unexpected volume variations
    """
    # Generate source-specific output key
    output_key = (
        f"volume_variation_results_{source_id}"
        if source_id
        else "volume_variation_results"
    )

    return LlmAgent(
        name="UnexpectedVolumeVariationDetector",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=UnexpectedVolumeVariationOutputSchema,
        output_key=output_key,  # Store results in session state with unique key
    )
