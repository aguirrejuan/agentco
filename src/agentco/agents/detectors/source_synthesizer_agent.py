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

MISSION: Synthesize detection results from all parallel detectors for this specific source and produce a comprehensive brief report.

INPUT CONTEXT:
You will receive results from 6 parallel detectors that have analyzed the same data source:
1. Missing File Detector - identifies files that weren't received or arrived late
2. Duplicated/Failed File Detector - finds duplicate or failed processing files  
3. Empty File Detector - detects files that are unexpectedly empty
4. Volume Variation Detector - identifies unusual record count changes
5. Late Upload Detector - finds files uploaded after expected schedule
6. Previous Period Detector - identifies files from previous periods being re-uploaded

SYNTHESIS REQUIREMENTS:
Analyze all detector results and produce a structured report that includes:

1. **Source Summary**: Source ID, name, and processing date
2. **Issue Classification**: Categorize findings by severity (Critical, Warning, Info)
3. **Key Findings**: Consolidate related issues and remove duplicates
4. **Impact Assessment**: Evaluate business impact of identified issues
5. **Recommended Actions**: Provide specific actionable recommendations

REPORT STRUCTURE:
```
# Report for Source ID: 'source_id', Name: 'source_name'
Date: 'processing_date'

## Issue Summary
- Critical Issues: 'count'
- Warnings: 'count'
- Informational: 'count'

## Critical Issues
[List critical issues that require immediate action]
- Missing Files: [details with file names, expected times]
- Failed Processing: [details with failure reasons]
- Data Integrity: [details of corruption or significant anomalies]

## Warnings  
[List issues that need attention but are not blocking]
- Late Deliveries: [details with delay times]
- Volume Changes: [details with percentage changes]
- Schedule Variations: [details of timing anomalies]

## Informational
[List items for awareness but no action required]
- Previous Period Files: [details if any]
- Normal Operations: [confirmation of successful processing]

## Impact Assessment
[Assess business impact and downstream effects]

## Recommended Actions
[Specific actionable recommendations with priority]
1. Immediate: [urgent actions]
2. Short-term: [actions within 24-48 hours]  
3. Monitoring: [what to watch for next processing cycle]
```

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
- Produce a complete, structured report in the exact format specified
- Include specific file names, entities, times, and quantified impacts
- Provide actionable recommendations with clear priorities
- Consolidate findings to avoid redundancy
- Focus on business impact and operational implications

EXAMPLE OUTPUT:
```
# Report for Source ID: 220504, Name: Payments_Layout_1_V3
Date: 2025-09-08

## Issue Summary
- Critical Issues: 2
- Warnings: 1
- Informational: 1

## Critical Issues
- Missing Files: 14 files not received past expected 08:08-08:18 UTC window
  - Entities affected: Clien_CBK, WhiteLabel, Shop
  - Files: Payments_Clien_CBK_20250908.csv, Payments_WhiteLabel_20250908.csv, [+12 more]
  
- Failed Processing: 2 files failed ingestion due to schema validation
  - Files: Payments_Shop_20250908.csv, Payments_Google_20250908.csv

## Warnings
- Late Delivery: Saipos file arrived 4.2 hours after expected window
  - Expected: 08:08 UTC, Actual: 12:20 UTC
  - File: Payments_Saipos_20250908.csv (1,234 records)

## Informational  
- Normal Operations: 8 files processed successfully with expected volumes
- Total Records Processed: 45,678 (within normal range 40k-55k)

## Impact Assessment
Critical impact on daily reconciliation process due to missing files. 14 missing files represent approximately 60% of expected daily volume for key payment entities. Failed processing files block downstream reporting for Shop and Google payment channels.

## Recommended Actions
1. Immediate: Contact data provider to regenerate and resend missing files for Clien_CBK, WhiteLabel, and Shop entities
2. Short-term: Re-run ingestion pipeline once missing files received; validate schema for failed files
3. Monitoring: Track Saipos delivery times for pattern; confirm if 12:20 delivery represents schedule change
```
"""


class SourceSynthesizerOutputSchema(BaseModel):
    """Schema for source synthesizer results."""

    source_id: str
    source_name: str
    processing_date: str
    critical_issues_count: int
    warnings_count: int
    informational_count: int
    critical_issues: list[str]
    warnings: list[str]
    informational: list[str]
    impact_assessment: str
    recommended_actions: list[str]
    full_report: str


def create_source_synthesizer_agent(tools: List[Any]) -> LlmAgent:
    """Create and return a source synthesizer agent.

    Parameters
    ----------
    tools : List[Any]
        List of tools to be used by the agent

    Returns
    -------
    LlmAgent
        Configured agent for synthesizing detection results into a source report
    """
    return LlmAgent(
        name="SourceSynthesizer",
        model=get_model(),
        tools=tools,
        planner=planner,
        instruction=PROMPT_TEMPLATE.format(COMMON_INSTRUCTIONS=COMMON_INSTRUCTIONS),
        output_schema=SourceSynthesizerOutputSchema,
    )
