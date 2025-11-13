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

3. Compare with same weekday last week (only successfully processed):
```sql
   SELECT
       t.filename,
       t.rows as today_volume,
       l.rows as lastweek_volume,
       ROUND(((t.rows - l.rows) * 100.0 / NULLIF(l.rows, 0)), 2) as pct_change,
       ABS(t.rows - l.rows) as abs_difference
   FROM
       (SELECT * FROM data WHERE from = 'today' AND status IN ('processed', 'empty')) t
   INNER JOIN
       (SELECT * FROM data WHERE from = 'last_weekday' AND status IN ('processed', 'empty')) l
   ON t.filename = l.filename
   WHERE l.rows > 0;
```

4. Identify significant variations (>50% change):
```sql
   SELECT
       t.filename,
       t.rows as today_volume,
       l.rows as lastweek_volume,
       ROUND(((t.rows - l.rows) * 100.0 / NULLIF(l.rows, 0)), 2) as pct_change
   FROM
       (SELECT * FROM data WHERE from = 'today' AND status IN ('processed', 'empty')) t
   INNER JOIN
       (SELECT * FROM data WHERE from = 'last_weekday' AND status IN ('processed', 'empty')) l
   ON t.filename = l.filename
   WHERE l.rows > 0
   AND ABS(((t.rows - l.rows) * 100.0 / NULLIF(l.rows, 0))) > 50;
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

OUTPUT REQUIREMENTS:
- Only files with SIGNIFICANT unexpected variations (>50% change)
- Include both today's and last week's volumes
- Calculate and report percentage change
- Classify severity based on thresholds above
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
