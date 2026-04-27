"""
API endpoints for quotation statistics
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.rfq import Job
from apps.app_nippon_rfq_matching.app.schemas.quotation_stats import (
    QuotationStatsResponse,
    QuotationSummaryResponse,
)

router = APIRouter(prefix="/quotation-stats", tags=["quotation-stats"])
logger = logging.getLogger(__name__)


@router.get("/summary", response_model=QuotationSummaryResponse)
async def get_quotation_summary(
    days: int = Query(
        30, ge=1, le=365, description="Number of days to include in summary"
    ),
    db: Session = Depends(get_db),
):
    """
    Get summary statistics for quotation exports

    - **days**: Number of days to include in summary (default: 30)

    Returns summary statistics including total quotations, success rate, etc.
    """
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Query database for job statistics
        query = db.query(Job).filter(
            Job.job_type == "quotation_export", Job.created_at >= start_date
        )

        total_jobs = query.count()

        # Count by status
        status_counts = {}
        for status in ["pending", "processing", "completed", "failed"]:
            status_counts[status] = query.filter(Job.status == status).count()

        # Count successful and failed jobs
        successful_jobs = query.filter(Job.status == "completed").count()
        failed_jobs = query.filter(Job.status == "failed").count()

        # Calculate success rate
        success_rate = (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0

        # Get average processing time for completed jobs
        completed_jobs = query.filter(Job.status == "completed").all()
        avg_processing_time = 0
        if completed_jobs:
            total_processing_time = 0
            for job in completed_jobs:
                if job.updated_at and job.created_at:
                    processing_time = (job.updated_at - job.created_at).total_seconds()
                    total_processing_time += processing_time
            avg_processing_time = (
                total_processing_time / len(completed_jobs) / 60
            )  # Convert to minutes

        return QuotationSummaryResponse(
            total_jobs=total_jobs,
            successful_jobs=successful_jobs,
            failed_jobs=failed_jobs,
            success_rate=round(success_rate, 2),
            average_processing_time=round(avg_processing_time, 2),
            status_counts=status_counts,
            days=days,
            period_start=start_date.isoformat(),
            period_end=end_date.isoformat(),
        )

    except Exception as e:
        logger.error(f"Error getting quotation summary: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error getting quotation summary: {str(e)}"
        )


@router.get("/daily", response_model=list[QuotationStatsResponse])
async def get_quotation_daily_stats(
    days: int = Query(30, ge=1, le=365, description="Number of days to include"),
    db: Session = Depends(get_db),
):
    """
    Get daily quotation export statistics

    - **days**: Number of days to include (default: 30)

    Returns daily breakdown of quotation exports
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Group by date and status
        daily_stats = (
            db.query(
                func.date(Job.created_at).label("date"),
                Job.status,
                func.count(Job.id).label("count"),
            )
            .filter(Job.job_type == "quotation_export", Job.created_at >= start_date)
            .group_by(func.date(Job.created_at), Job.status)
            .order_by(func.date(Job.created_at))
            .all()
        )

        # Create daily summary
        summary = {}
        for stat in daily_stats:
            # Convert stat.date to datetime.date object if it's a string
            if isinstance(stat.date, str):
                date_obj = datetime.strptime(stat.date, "%Y-%m-%d").date()
            else:
                date_obj = stat.date
            date_str = date_obj.isoformat()
            if date_str not in summary:
                summary[date_str] = {
                    "date": date_str,
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                    "pending": 0,
                    "processing": 0,
                }

            summary[date_str][stat.status] = stat.count
            summary[date_str]["total"] += stat.count

        return list(summary.values())

    except Exception as e:
        logger.error(f"Error getting daily quotation stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error getting daily quotation stats: {str(e)}"
        )


@router.get("/trends", response_model=dict[str, Any])
async def get_quotation_trends(
    days: int = Query(30, ge=1, le=365, description="Number of days to include"),
    db: Session = Depends(get_db),
):
    """
    Get quotation export trends

    - **days**: Number of days to include (default: 30)

    Returns trend data for quotation exports
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Query total counts by date
        trend_data = (
            db.query(
                func.date(Job.created_at).label("date"),
                func.count(Job.id).label("total"),
            )
            .filter(Job.job_type == "quotation_export", Job.created_at >= start_date)
            .group_by(func.date(Job.created_at))
            .order_by(func.date(Job.created_at))
            .all()
        )

        # Calculate trends
        total_quotations = sum([t.total for t in trend_data])
        avg_quotations_per_day = total_quotations / days if days > 0 else 0

        # Success rate trend
        successful_quotations = (
            db.query(
                func.date(Job.created_at).label("date"),
                func.count(Job.id).label("count"),
            )
            .filter(
                Job.job_type == "quotation_export",
                Job.status == "completed",
                Job.created_at >= start_date,
            )
            .group_by(func.date(Job.created_at))
            .all()
        )

        # Convert to dictionary for easier processing
        success_by_date = {}
        for s in successful_quotations:
            if isinstance(s.date, str):
                date_obj = datetime.strptime(s.date, "%Y-%m-%d").date()
            else:
                date_obj = s.date
            success_by_date[date_obj.isoformat()] = s.count

        trend_by_date = {}
        for t in trend_data:
            if isinstance(t.date, str):
                date_obj = datetime.strptime(t.date, "%Y-%m-%d").date()
            else:
                date_obj = t.date
            trend_by_date[date_obj.isoformat()] = t.total

        # Calculate daily success rates
        daily_success_rates = []
        for date_str, total in trend_by_date.items():
            success = success_by_date.get(date_str, 0)
            success_rate = (success / total * 100) if total > 0 else 0
            daily_success_rates.append(
                {
                    "date": date_str,
                    "success_rate": round(success_rate, 2),
                    "total_quotations": total,
                    "successful_quotations": success,
                }
            )

        return {
            "period": {
                "days": days,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "summary": {
                "total_quotations": total_quotations,
                "average_quotations_per_day": round(avg_quotations_per_day, 2),
                "days_with_quotations": len(trend_data),
            },
            "daily_success_rates": daily_success_rates,
        }

    except Exception as e:
        logger.error(f"Error getting quotation trends: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error getting quotation trends: {str(e)}"
        )
