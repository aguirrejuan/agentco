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

MISSION: Synthesize detection results from all 6 parallel detectors for THIS SPECIFIC SOURCE into a structured single-source report that will be consumed by the final multi-source executive report generator.

**CRITICAL UNDERSTANDING**:
- You are analyzing ONE data source only
- Your output will be combined with OTHER source reports by a final synthesizer
- The final synthesizer needs specific fields from you in exact formats
- Your "Summary Line" will become a bullet point in the final executive report

INPUT CONTEXT:
You will receive results from 6 parallel detectors that have analyzed the SAME data source and stored their findings in session state.

**Detection Results Available:**


Your job is to synthesize these detection findings into a source-specific report using the three-tier classification system that aligns with the final report structure.

CRITICALITY CLASSIFICATION (MUST ALIGN WITH FINAL REPORT):

🚨 **URGENT ACTION REQUIRED** - Sources with:
- 2+ files missing from same source
- Critical duplicated/failed files blocking processing
- Volume changes >100% increase or >80% decrease
- Multiple files failed with same error pattern
- Any processing-blocking issues

⚠️ **NEEDS ATTENTION** - Sources with:
- 1 file missing past expected window
- Volume variations 50-100% from normal
- Files arriving significantly late (>4 hours)
- Unexpected empty files that historically had data
- Schedule changes or timing anomalies

✅ **NO ACTION NEEDED** - Sources with:
- All files received and processed normally
- Volumes within acceptable ranges (±50%)
- No duplicates, failures, or timing issues
- Previous period uploads (informational only)

SYNTHESIS REQUIREMENTS:
Analyze all detector results and produce a structured report that includes:

1. **Source Summary**: Source ID, name, and processing date
2. **Criticality Level**: Determine the highest severity level (Urgent/Attention/Normal)
3. **Key Findings**: Consolidate related issues with specific details
4. **Total Record Count**: If normal operations, provide total records processed
5. **Recommended Actions**: Specific actionable recommendations (only for Urgent/Attention)

REPORT STRUCTURE:
```
## Source: [source_name] (id: [source_id])
**Date**: [processing_date]
**Criticality Level**: [URGENT ACTION REQUIRED | NEEDS ATTENTION | NO ACTION NEEDED]

### Summary Line
**CRITICAL**: This line will become the bullet point content in the final executive report.
Format: • *[source_name] (id: [source_id])* – [date]: [your summary line here]

Your summary line must be:
- Concise and specific (one line only)
- Include key details: counts, time windows, entity names, volumes
- Match the exact format patterns below:

**For URGENT issues (missing files)**:
"14 files missing past 08:08–08:18 UTC window — entities: Clien_CBK, WhiteLabel, Shop, Google..."
"2 files missing past 08:02–08:11 UTC — expected: *_Clien_Debito_payments_accounting_report_2025_09_07.csv; *_Clien_MVP_payments_accounting_report_2025_09_07.csv"

**For ATTENTION issues (volume/timing)**:
"ClienX volume 61,639 (> usual Monday 40k–55k)"
"ClienX volume 56,277 (>95% bound 50,211)"
"Saipos file delivered early at 08:06 UTC (usual ~17:20)"
"ClienX 1,023,337 (>95% band 869,600) and lag shifted to near-real-time"

**For NORMAL operations**:
"`[1,233,496] records`"

DO NOT include source name or ID in your summary line - the final synthesizer adds that.

### Detailed Findings
[Specific details organized by issue type]

**Missing Files** (if any):
- Count: [X files missing]
- Time Window: [expected arrival window, e.g., "08:08-08:18 UTC"]
- Entities Affected: [list entities: Clien_CBK, WhiteLabel, Shop, etc.]
- Expected Filenames: [use * or [hash] for variable prefixes]

**Failed/Duplicate Files** (if any):
- Specific files and error patterns
- Processing status (failure, stopped, deleted)

**Volume Variations** (if any):
- Entity name and volume with comparison
- Format: "EntityName volume X (comparison to baseline)"
- Reference CV ranges when available

**Late/Early Uploads** (if any):
- Entity name and timing details
- Actual vs expected time
- Hours delayed/early

**Empty Files** (if any):
- Files unexpectedly empty
- Historical comparison

**Previous Period Files** (if any):
- Informational only - note age and context

### Total Records
[If normal operations]: `[X,XXX] records`

### Recommended Action
[Only for URGENT or NEEDS ATTENTION - leave empty for NO ACTION NEEDED]
**CRITICAL**: Use EXACT action templates that match issue type:

**For Missing Files (URGENT)**:
"Notify provider to generate/re-send; re-run ingestion and verify completeness"

**For Volume Anomalies (ATTENTION)**:
"Confirm coverage/window; monitor next run"

**For Early/Late Uploads (ATTENTION)**:
"Confirm schedule change; adjust downstream triggers if needed"
"Validate downstream completed; track if persists"
"Confirm intentional lag change; keep a short-term volume watch"

**For NO ACTION NEEDED**:
Leave blank or state "None"

Format in final report: → *Action:* [your recommended action]
```

ANALYSIS GUIDELINES:

**Determine Criticality Level:**
1. Check all detector results
2. Identify the HIGHEST severity issue found
3. Classify the entire source based on that level
4. Use the classification criteria above

**Issue Classification By Detector:**
- **Missing Files**: URGENT if 2+, ATTENTION if 1
- **Duplicates/Failures**: URGENT if blocking processing, ATTENTION otherwise
- **Volume Variations**: URGENT if >100% increase or >80% decrease, ATTENTION if 50-100%
- **Late Uploads**: ATTENTION (files >4 hours late), INFORMATIONAL if early arrivals
- **Empty Files**: URGENT if multiple critical files, ATTENTION if single file
- **Previous Period**: Usually INFORMATIONAL unless blocking

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

HOW YOUR OUTPUT FEEDS THE FINAL REPORT:

The final multi-source synthesizer will extract from your report:
1. **criticality_level** → Determines which section (Urgent/Attention/Normal)
2. **summary_line** → Becomes the bullet point description
3. **recommended_action** → Appended as "→ *Action:* [text]"

Final format: • *SourceName (id: XXXXX)* – YYYY-MM-DD: [YOUR SUMMARY LINE] → *Action:* [YOUR ACTION]

Example transformation:
```
Your output:
- criticality_level: "NEEDS ATTENTION"
- summary_line: "ClienX volume 61,639 (> usual Monday 40k–55k)"
- recommended_action: "Confirm coverage/window; monitor next run"

Becomes in final report:
* Needs Attention*
• *Sale_adjustments_3 (id: 239611)* – 2025-09-08: ClienX volume 61,639 (> usual Monday 40k–55k) → *Action:* Confirm coverage/window; monitor next run
```

OUTPUT REQUIREMENTS:
- Produce a complete, structured report in the exact format specified above
- **CRITICAL**: Your summary_line field is the MOST IMPORTANT - it becomes the visible description in the final report
- Include specific file names, entities, time windows, and quantified values in summary line
- Use exact formatting for final report compatibility:
  - Time windows: "08:08-08:18 UTC"
  - Record counts: "`[X,XXX] records`" (with backticks and brackets)
  - Entity lists: "entities: Name1, Name2, Name3..."
  - Comparisons: "volume X (> usual Y-Z)" or "(>95% bound Z)"
  - Use em dash "—" (not hyphen) to separate clauses
- Consolidate findings to avoid redundancy
- Provide actionable recommendations using EXACT standard templates
- State criticality level explicitly (exactly one of: "URGENT ACTION REQUIRED", "NEEDS ATTENTION", "NO ACTION NEEDED")

EXAMPLE OUTPUT 1 (URGENT):
```
## Source: Payments_Layout_1_V3 (id: 220504)
**Date**: 2025-09-08
**Criticality Level**: URGENT ACTION REQUIRED

### Summary Line
14 files missing past 08:08–08:18 UTC window — entities: Clien_CBK, WhiteLabel, Shop, Google, POC, Market, Innovation, Donation, Beneficios, ApplePay, Anota-ai, AddCard, Clien_payments, ClienX_Clube

### Detailed Findings

**Missing Files**:
- Count: 14 files missing
- Time Window: 08:08-08:18 UTC
- Entities Affected: Clien_CBK, WhiteLabel, Shop, Google, POC, Market, Innovation, Donation, Beneficios, ApplePay, Anota-ai, AddCard, Clien_payments, ClienX_Clube
- All files follow pattern: [hash]_EntityName_payments_accounting_report_2025_09_08.csv

**Failed/Duplicate Files**:
- 2 files with status = 'failure' due to schema validation errors
- Files: [hash]_Shop_payments_20250908.csv, [hash]_Google_payments_20250908.csv

### Recommended Actions
**Action**: Notify provider to generate/re-send; re-run ingestion and verify completeness
```

EXAMPLE OUTPUT 2 (NEEDS ATTENTION):
```
## Source: Sale_adjustments_3 (id: 239611)
**Date**: 2025-09-08
**Criticality Level**: NEEDS ATTENTION

### Summary Line
ClienX volume 61,639 (> usual Monday 40k–55k)

### Detailed Findings

**Volume Variations**:
- Entity: ClienX
- Today's Volume: 61,639 records
- Expected Range: 40,000-55,000 (usual Monday pattern)
- Deviation: +23% above upper bound

### Recommended Actions
**Action**: Confirm coverage/window; monitor next run
```

EXAMPLE OUTPUT 3 (NO ACTION NEEDED):
```
## Source: Sale_payments_2 (id: 228036)
**Date**: 2025-09-08
**Criticality Level**: NO ACTION NEEDED

### Summary Line
`[1,233,496] records`

### Detailed Findings
All files received and processed successfully. Volumes within expected ranges. No duplicates, failures, or timing issues detected.

### Total Records
`[1,233,496] records`
```
"""


class SourceSynthesizerOutputSchema(BaseModel):
    """Schema for source synthesizer results aligned with final report format."""

    source_id: str
    source_name: str
    processing_date: str
    criticality_level: str  # "URGENT ACTION REQUIRED" | "NEEDS ATTENTION" | "NO ACTION NEEDED"
    summary_line: str  # One-line summary for final report
    missing_files_details: str  # Details about missing files (if any)
    failed_duplicate_details: str  # Details about failures/duplicates (if any)
    volume_variation_details: str  # Details about volume issues (if any)
    late_early_upload_details: str  # Details about timing issues (if any)
    empty_file_details: str  # Details about empty files (if any)
    previous_period_details: str  # Details about previous period files (if any)
    total_records: str  # Total records if normal operations (e.g., "[1,233,496] records")
    recommended_action: str  # Recommended action (empty if NO ACTION NEEDED)
    full_report: str  # Complete formatted report


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
        include_contents="none",
        instruction=formatted_instruction,  # Session state will be injected automatically
        output_schema=SourceSynthesizerOutputSchema,
        output_key=output_key,  # Store result in session state if key provided
    )
