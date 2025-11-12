"""Data Quality Detection Agents

This module contains various agents for detecting different types of data quality issues
in file processing workflows.
"""

from .duplicated_failed_detector_agent import (
    DuplicatedAndFailedFileOutputSchema,
    create_duplicated_and_failed_file_detector_agent,
)
from .empty_file_detector_agent import (
    UnexpectedEmptyFileOutputSchema,
    create_unexpected_empty_file_detector_agent,
)
from .late_upload_detector_agent import (
    FileUploadAfterScheduleOutputSchema,
    create_file_upload_after_schedule_detector_agent,
)
from .missing_detector_agent import (
    MissingFileOutputSchema,
    create_missing_file_detector_agent,
)
from .previous_period_detector_agent import (
    UploadOfPreviousFileOutputSchema,
    create_upload_of_previous_file_detector_agent,
)
from .volume_variation_detector_agent import (
    UnexpectedVolumeVariationOutputSchema,
    create_unexpected_volume_variation_detector_agent,
)

__all__ = [
    # Factory functions - primary interface
    "create_missing_file_detector_agent",
    "create_duplicated_and_failed_file_detector_agent",
    "create_unexpected_empty_file_detector_agent",
    "create_unexpected_volume_variation_detector_agent",
    "create_file_upload_after_schedule_detector_agent",
    "create_upload_of_previous_file_detector_agent",
    # Output schemas
    "MissingFileOutputSchema",
    "DuplicatedAndFailedFileOutputSchema",
    "UnexpectedEmptyFileOutputSchema",
    "UnexpectedVolumeVariationOutputSchema",
    "FileUploadAfterScheduleOutputSchema",
    "UploadOfPreviousFileOutputSchema",
]
