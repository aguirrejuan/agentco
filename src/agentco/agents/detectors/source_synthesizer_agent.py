"""Source Synthesizer Agent

This module contains the agent responsible for synthesizing results from all parallel detectors
for a specific source and producing a brief report with key findings.
"""

from typing import Any, List

from google.adk.agents import LlmAgent
from google.adk.planners import BuiltInPlanner
from google.genai.types import ThinkingConfig
from pydantic import BaseModel

from ...logger import logger
from ..commons import COMMON_INSTRUCTIONS, get_model

# Thinking configuration
thinking_config = ThinkingConfig(include_thoughts=True, thinking_budget=512)

# Instantiate BuiltInPlanner
planner = BuiltInPlanner(thinking_config=thinking_config)

PROMPT_TEMPLATE = """
{COMMON_INSTRUCTIONS}

MISSION: Synthesize detection results from all parallel detectors for this specific source and produce a concise summary for final report generation.

INPUT CONTEXT:
You will receive results from 6 parallel detectors that have analyzed the same data source and stored their findings in session state.

**Detection Results Available:**


Your job is to synthesize these detection findings into a concise output format.

SYNTHESIS REQUIREMENTS:
Analyze all detector results and produce:

1. **Issue Counts**: Count critical, warning, and informational issues
2. **Issues Summary**: 2-3 sentence consolidated summary of key problems
3. **Impact**: 1-2 sentence business impact assessment  
4. **Actions**: Maximum 3 priority recommendations

OUTPUT FORMAT:
Produce concise structured output matching the schema:

- **Issues Summary**: Brief 2-3 sentence overview of key findings (missing files, failures, delays, etc.)
- **Impact**: 1-2 sentence assessment of business/operational impact
- **Actions**: Maximum 3 priority recommendations, ordered by urgency

Keep descriptions focused and actionable. Avoid detailed file listings or verbose explanations.

ANALYSIS GUIDELINES:

**Issue Classification:**
- **Critical**: Missing files, failed processing, data corruption, processing blocks
- **Warning**: Late files (>4h delay), volume changes >50%, schedule anomalies
- **Info**: Normal operations, previous period uploads (if not blocking), minor variations

**Consolidation Logic:**
- Group related issues (e.g., multiple missing files from same entity)
- Avoid duplicate reporting (e.g., if a file is both missing AND late, report as missing)
- Identify patterns across different detection types
- Prioritize by business impact

**Entity Extraction:**
Extract business entities from filenames: Clien_CBK, WhiteLabel, Shop, Google, POC, Market, Innovation, Donation, Beneficios, ApplePay, Anota-ai, AddCard, Clien_payments, ClienX_Clube, Saipos, etc.

**Time Analysis:**
- Calculate delays in hours/minutes
- Reference expected schedules from CV documentation
- Note day-of-week patterns and exceptions

**Volume Analysis:**  
- Calculate percentage changes from normal baselines
- Identify trends (increasing, decreasing, stable)
- Note if changes are within acceptable business ranges

OUTPUT REQUIREMENTS:
- Be concise - prioritize brevity over detail
- Focus on actionable insights, not exhaustive descriptions
- Consolidate similar issues into summary statements
- Limit actions to top 3 priorities only
- Use quantified impacts where relevant but keep descriptions short

EXAMPLE OUTPUT:
Source: 220504, Payments_Layout_1_V3, 2025-09-08
Counts: 2 critical, 1 warning, 1 info

Issues Summary: 14 files missing from Clien_CBK, WhiteLabel, and Shop entities past expected 08:08 UTC window. 2 files failed schema validation. Saipos delivery delayed 4.2 hours but processed successfully.

Impact: Critical impact on daily reconciliation - 60% of expected volume missing, blocking downstream reporting for key payment channels.

Actions:
1. Contact provider to regenerate missing files immediately
2. Re-run ingestion pipeline once files received  
3. Monitor Saipos delivery pattern for schedule changes
"""


class SourceSynthesizerOutputSchema(BaseModel):
    """Schema for source synthesizer results - concise output for final report generation."""

    source_id: str
    source_name: str
    processing_date: str
    critical_count: int
    warning_count: int
    info_count: int
    issues_summary: str  # Brief consolidated summary of all issues
    impact: str  # Concise impact assessment
    actions: list[str]  # Top 3 recommended actions only


def create_source_synthesizer_agent(
    tools: List[Any] = None, output_key: str = None, source_id: str = None
) -> LlmAgent:
    """Create and return a source synthesizer agent.

    Parameters
    ----------
    tools : List[Any], optional
        Not used for synthesis - detection results come from session state
    output_key : str, optional
        Key to store the synthesizer output in session state for multi-source synthesis
    source_id : str, optional
        Source identifier for reading source-specific detection results

    Returns
    -------
    LlmAgent
        Configured agent for synthesizing detection results into a source report
    """
    logger.debug(
        f"Creating SourceSynthesizerAgent for source_id={source_id} with output_key={output_key}"
    )
    # Build dynamic instruction with source-specific keys
    if source_id:
        detection_results_section = f"""
**Detection Results Available:**
- **Missing File Results**: {{{{missing_file_results_{source_id}}}}}
- **Duplicated/Failed Results**: {{{{duplicated_failed_results_{source_id}}}}} 
- **Empty File Results**: {{{{empty_file_results_{source_id}}}}}
- **Volume Variation Results**: {{{{volume_variation_results_{source_id}}}}}
- **Late Upload Results**: {{{{late_upload_results_{source_id}}}}}
- **Previous Period Results**: {{{{previous_period_results_{source_id}}}}}
"""
    else:
        # Fallback for backward compatibility
        detection_results_section = """
**Detection Results Available:**
- **Missing File Results**: {{missing_file_results}}
- **Duplicated/Failed Results**: {{duplicated_failed_results}} 
- **Empty File Results**: {{empty_file_results}}
- **Volume Variation Results**: {{volume_variation_results}}
- **Late Upload Results**: {{late_upload_results}}
- **Previous Period Results**: {{previous_period_results}}
"""
    logger.debug(
        f"Detection results section for SourceSynthesizerAgent:\n{detection_results_section}"
    )

    # Replace the placeholder in the template
    source_specific_template = PROMPT_TEMPLATE.replace(
        "**Detection Results Available:**",
        detection_results_section.strip(),
    )

    # Pre-format the template with COMMON_INSTRUCTIONS to avoid conflicts with session state injection
    formatted_instruction = source_specific_template.format(
        COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS
    )
    logger.debug(f"Prompt for SourceSynthesizerAgent:\n{formatted_instruction}")
    return LlmAgent(
        name="SourceSynthesizer",
        model=get_model(),
        tools=[],  # No tools needed - reading from session state
        planner=planner,
        instruction=formatted_instruction,  # Session state will be injected automatically
        output_schema=SourceSynthesizerOutputSchema,
        output_key=output_key,  # Store result in session state if key provided
    )
