# AgentCo 

![system_overview](./assets/agentco.png)

![system_agents](./assets/agents-sys.png)

A data quality analysis agent that automatically detects issues in data ingestion pipelines and generates executive reports.

## Overview

AgentCo is an AI-powered data quality monitoring tool that analyzes data sources, detects anomalies, and produces actionable executive reports. It uses multiple detection agents to identify common data quality issues like missing files, duplicates, empty files, and volume variations.

## Features

- **Multi-Source Analysis**: Automatically discovers and analyzes multiple data sources
- **Comprehensive Detection**: Six types of data quality detectors per source:
  - Missing file detection
  - Duplicate and failed file detection  
  - Empty file detection
  - Volume variation detection
  - Late upload detection
  - Previous period file detection
- **Smart Synthesis**: Individual source reports plus executive summary:
  - Source-specific synthesis after parallel detection
  - Issue classification (Critical, Warning, Informational)
  - Impact assessment and actionable recommendations
  - Executive-level multi-source synthesis
- **Executive Reports**: Generates clear, actionable reports with priority levels
- **CLI Interface**: Easy-to-use command line tool
- **Parallel Processing**: Efficient analysis of multiple sources simultaneously

## Installation

```bash
uv pip install -e .
```

## Quick Start

```bash
# Analyze data quality for your sources
agentco analyze /path/to/cv_files /path/to/json_files

# Limit analysis to 3 sources and save output
agentco analyze ./cv_files ./json_files --max-sources 3 --save-output

# Get help
agentco --help
```

## Usage

### Basic Analysis

```bash
agentco analyze <cv_folder> <json_folder>
```

### Options

- `--extract-names/--no-extract-names`: Extract source names from CV file headers (default: true)
- `--max-sources <n>`: Limit analysis to n sources (useful for testing)
- `--save-output/--no-save-output`: Save report to file (default: false)  
- `--session-id <id>`: Custom session ID (default: auto-generated)

### Example Output

The tool generates executive reports with three priority levels:

- **🚨 Urgent Action Required**: Critical issues blocking processing
- **⚠️ Needs Attention**: Issues requiring monitoring or review
- **✅ No Action Needed**: Sources operating normally

## Requirements

- Python ≥ 3.12
- Google AI Development Kit (ADK)
- Environment variables for AI model access

## Architecture

AgentCo uses a sophisticated multi-tier analysis architecture:

### Analysis Pipeline
1. **Parallel Detection**: 6 detectors run simultaneously per source
2. **Source Synthesis**: Individual reports with structured findings
3. **Executive Synthesis**: Cross-source executive summary

### Agent Types
- **Detector Agents**: Specialized detection for specific issue types
- **Source Synthesizer**: Consolidates detection results into structured source reports
- **Multi-Source Synthesizer**: Creates executive summaries from multiple source reports

## Project Structure

```
src/agentco/
├── cli.py                          # Command line interface
├── agents/                         # Agent implementations
│   ├── detectors/                  # Detection and synthesis agents
│   │   ├── missing_detector_agent.py
│   │   ├── duplicated_failed_detector_agent.py
│   │   ├── empty_file_detector_agent.py
│   │   ├── volume_variation_detector_agent.py
│   │   ├── late_upload_detector_agent.py
│   │   ├── previous_period_detector_agent.py
│   │   └── source_synthesizer_agent.py    # NEW: Individual source synthesis
│   ├── factory.py                  # Agent creation and pipeline configuration
│   └── commons.py                  # Shared agent utilities
├── data/                           # Data processing utilities
└── tools.py                       # Analysis tools and utilities
```

## Advanced Usage

### Individual Source Analysis
```python
from src.agentco.agents.factory import create_single_source_complete_analysis

# Analyze a single source with detection + synthesis
pipeline = create_single_source_complete_analysis(
    source_id="220504",
    day_folder=Path("data/2025-09-08"),
    datasource_folder=Path("cvs/"),
    source_name="Payments_Layout_1_V3"
)
```

### Custom Multi-Source Pipeline
```python
from src.agentco.agents.factory import create_multi_source_detection_pipeline

# Custom multi-source configuration
sources = [
    {
        "source_id": "220504", 
        "name": "Payments_Layout_1_V3",
        "day_folder": Path("data/2025-09-08"),
        "datasource_folder": Path("cvs/")
    }
]

pipeline = create_multi_source_detection_pipeline(sources)
```

## License

This project is developed by Juan Carlos Aguirre Arango.