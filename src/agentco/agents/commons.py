"""Common utilities and configurations for agent modules."""

from pathlib import Path
from typing import Any, List

from dotenv import load_dotenv
from google.adk.models.lite_llm import LiteLlm

from agentco.tools import DataSourceToolset

load_dotenv()


def get_model() -> LiteLlm:
    """Get a configured LLM model instance.

    Returns
    -------
    LiteLlm
        Configured LiteLLM instance
    """
    return LiteLlm(model="openai/gpt-4.1", temperature=0)


def get_tools(source_id: str, day_folder: Path, datasource_folder: Path) -> List[Any]:
    """Get default tools configuration for agents.

    Parameters
    ----------
    source_id : str, default="195385"
        The source identifier
    day_folder : Path, default=Path('...')
        Path to the day folder containing data files
    datasource_folder : Path, default=Path('...')
        Path to the datasource folder containing CV files

    Returns
    -------
    List[Any]
        List of configured tools
    """
    data_tools = DataSourceToolset(
        source_id=source_id, day_folder=day_folder, datasource_folder=datasource_folder
    )
    return [data_tools]


# Common instructions for all agents
COMMON_INSTRUCTIONS = """
ANALYSIS PROCESS:
1. First, read the data source CV using read_data_source_cv() to understand:
   - Expected file patterns and naming conventions
   - Scheduled arrival times
   - Expected volume patterns
   - Normal vs. abnormal behaviors

2. Query the data systematically:
   - Start with aggregations to get overview statistics
   - Use DISTINCT, COUNT, SUM to minimize data transfer
   - Compare today's data with last_weekday data when needed

3. Apply critical thinking:
   - Consider day-of-week patterns (weekdays vs. weekends)
   - Account for known exceptions documented in the CV
   - Focus on significant deviations, not minor variations

OUTPUT FORMAT:
- Return specific filenames with clear issue descriptions
- Include relevant metrics (counts, times, volumes)
- Be concise but informative
"""
