"""
Quotation Statistics Schemas
"""

from pydantic import BaseModel, Field


class QuotationStatsResponse(BaseModel):
    """Response schema for daily quotation statistics"""

    date: str = Field(..., description="Date in YYYY-MM-DD format")
    total: int = Field(0, description="Total quotations for the day")
    completed: int = Field(0, description="Successful quotations")
    failed: int = Field(0, description="Failed quotations")
    pending: int = Field(0, description="Pending quotations")
    processing: int = Field(0, description="Processing quotations")


class StatusCount(BaseModel):
    """Status count model"""

    pending: int = Field(0, description="Number of pending jobs")
    processing: int = Field(0, description="Number of processing jobs")
    completed: int = Field(0, description="Number of completed jobs")
    failed: int = Field(0, description="Number of failed jobs")


class QuotationSummaryResponse(BaseModel):
    """Response schema for quotation summary statistics"""

    total_jobs: int = Field(..., description="Total number of quotation jobs")
    successful_jobs: int = Field(..., description="Number of successful quotation jobs")
    failed_jobs: int = Field(..., description="Number of failed quotation jobs")
    success_rate: float = Field(..., description="Success rate percentage")
    average_processing_time: float = Field(
        ..., description="Average processing time in minutes"
    )
    status_counts: StatusCount = Field(..., description="Counts by status")
    days: int = Field(..., description="Number of days included in summary")
    period_start: str = Field(..., description="Start date of the period")
    period_end: str = Field(..., description="End date of the period")
