"""
Job service for managing async background tasks
"""

import asyncio
import json
import threading
import uuid
from collections import deque
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.logging_config import get_logger
from apps.app_nippon_rfq_matching.app.models.rfq import Job
from apps.app_nippon_rfq_matching.app.services.competitor_color_excel_service import (
    competitor_color_excel_service,
)
from apps.app_nippon_rfq_matching.app.services.competitor_color_service import (
    competitor_color_service,
)
from apps.app_nippon_rfq_matching.app.services.pdf_comparison_export_service import (
    pdf_comparison_export_service,
)
from apps.app_nippon_rfq_matching.app.services.quotation_pdf_service import (
    generate_quotation_background,
)
from apps.app_nippon_rfq_matching.app.services.rfq_service import rfq_service

logger = get_logger(__name__)


# ============================================================================
# SEQUENTIAL JOB QUEUE
# ============================================================================


class SequentialJobQueue:
    """
    Sequential job queue that processes jobs one at a time.

    Jobs are added to the queue and processed in FIFO order.
    Only one job is processed at a time, ensuring no concurrent processing.
    """

    def __init__(self):
        self._queue: deque = deque()
        self._processing = False
        self._lock = threading.Lock()
        self._worker_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def add_job(self, job_id: str, **kwargs):
        """
        Add a job to the queue

        Args:
            job_id: Job ID to process
            **kwargs: Additional job parameters (rfq_id, format_type, etc.)
        """
        with self._lock:
            self._queue.append({"job_id": job_id, "kwargs": kwargs})
            logger.info(f"Job {job_id} added to queue. Queue size: {len(self._queue)}")

            # Start worker if not running
            if not self._processing:
                self._start_worker()

    def _start_worker(self):
        """Start the background worker thread"""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._processing = True
            self._stop_event.clear()
            self._worker_thread = threading.Thread(
                target=self._worker_loop, daemon=True, name="SequentialJobQueueWorker"
            )
            self._worker_thread.start()
            logger.info("Sequential job queue worker started")

    def _worker_loop(self):
        """Worker loop that processes jobs sequentially"""
        while not self._stop_event.is_set():
            job_data = None

            # Get next job from queue
            with self._lock:
                if self._queue:
                    job_data = self._queue.popleft()
                elif not self._queue:
                    # No more jobs, stop processing
                    self._processing = False
                    break

            if job_data:
                job_id = job_data["job_id"]
                kwargs = job_data["kwargs"]

                logger.info(f"Processing job {job_id} (queue size: {len(self._queue)})")

                # Process the job
                try:
                    # Run the async job in this thread
                    run_async_task(job_processor.start_job_safe(job_id, **kwargs))
                    logger.info(f"Job {job_id} completed")
                except Exception as e:
                    logger.error(f"Error processing job {job_id}: {e}", exc_info=True)

                # Small delay between jobs
                self._stop_event.wait(0.1)

        logger.info("Sequential job queue worker stopped")

    def stop(self):
        """Stop the worker and clear the queue"""
        self._stop_event.set()
        with self._lock:
            self._queue.clear()
            self._processing = False

    def get_queue_size(self) -> int:
        """Get current queue size"""
        with self._lock:
            return len(self._queue)

    def is_processing(self) -> bool:
        """Check if worker is currently processing"""
        return self._processing


# Global sequential job queue instance
sequential_job_queue = SequentialJobQueue()


def get_db_session():
    """Create a new database session for background tasks"""
    from apps.app_nippon_rfq_matching.app.core.database import (
        SessionLocal,
        _ensure_db_initialized,
    )

    _ensure_db_initialized()
    return SessionLocal()


def run_async_task(coro):
    """
    Run an async coroutine in a new event loop in a background thread

    Args:
        coro: Coroutine to run
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(coro)
    finally:
        loop.close()


class JobService:
    """Service for managing async background jobs"""

    def create_job(self, job_type: str, file_path: str, db: Session) -> Job:
        """
        Create a new job

        Args:
            job_type: Type of job ('rfq_parse_store' or 'iatp_parse_store')
            file_path: Path to the uploaded file
            db: Database session

        Returns:
            Created Job record
        """
        job = Job(
            job_id=str(uuid.uuid4()),
            job_type=job_type,
            status="pending",
            file_path=file_path,
            progress=0,
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        return job

    def update_job_status(
        self,
        job_id: str,
        status: str,
        db: Session,
        progress: int | None = None,
        result: dict[str, Any] | None = None,
        error_message: str | None = None,
    ):
        """
        Update job status

        Args:
            job_id: Job ID
            status: New status
            db: Database session
            progress: Progress percentage (0-100)
            result: Result data (will be JSON serialized)
            error_message: Error message if failed
        """
        job = db.query(Job).filter(Job.job_id == job_id).first()
        if job:
            job.status = status
            if progress is not None:
                job.progress = progress
            if result is not None:
                job.result = json.dumps(result)
            if error_message is not None:
                job.error_message = error_message
            if status == "completed":
                job.completed_at = datetime.utcnow()
            db.commit()
            db.refresh(job)

    def get_job(self, job_id: str, db: Session) -> Job | None:
        """Get job by job_id"""
        return db.query(Job).filter(Job.job_id == job_id).first()

    def get_job_dict(self, job_id: str, db: Session) -> dict[str, Any] | None:
        """Get job as dictionary, with parsed result"""
        import time

        start_time = time.time()

        job = self.get_job(job_id, db)
        if not job:
            return None

        elapsed = time.time() - start_time
        if elapsed > 1.0:
            logger.warning(f"Slow job query for {job_id}: {elapsed:.2f}s")

        result = job.to_dict()
        # Parse JSON result to separate parameters and actual result
        if job.result:
            try:
                result_data = json.loads(job.result)
                # Check if this is parameters (has request_data key)
                if isinstance(result_data, dict) and "request_data" in result_data:
                    result["parameters"] = result_data
                # Check if this is actual result (has success key)
                elif isinstance(result_data, dict) and "success" in result_data:
                    result["result_data"] = result_data
                else:
                    result["result_data"] = result_data
            except Exception:
                result["result_data"] = None
        return result

    def add_job_parameters(self, job_id: str, parameters: dict[str, Any], db: Session):
        """Add parameters to job for background processing"""
        job = self.get_job(job_id, db)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        # Store parameters as JSON in result field temporarily
        job.result = json.dumps(parameters)
        try:
            db.commit()
            logger.info(f"Successfully added parameters to job {job_id}")
        except Exception as e:
            logger.error(f"Failed to commit parameters for job {job_id}: {e}")
            raise


# Background job processor
class JobProcessor:
    """Process background jobs"""

    def __init__(self, job_service: JobService):
        self.job_service = job_service
        self._running = False

    async def process_rfq_job(
        self, job_id: str, file_path: str, rfq_id: str, db: Session
    ):
        """
        Process RFQ parse and store job

        Args:
            job_id: Job ID
            file_path: Path to PDF file
            rfq_id: RFQ identifier
            db: Database session
        """
        try:
            # Get job to access uploaded_file_id
            job = self.job_service.get_job(job_id, db)

            # Update status to processing
            self.job_service.update_job_status(job_id, "processing", db, progress=10)

            # Parse PDF
            self.job_service.update_job_status(job_id, "processing", db, progress=30)
            rfq_items = await rfq_service.parse_pdf_file(file_path, rfq_id, db)

            # Save to database with uploaded_file_id
            self.job_service.update_job_status(job_id, "processing", db, progress=70)
            db_items = rfq_service.save_rfq_items_to_db(rfq_items, rfq_id, job.id, db)

            # Perform matching
            self.job_service.update_job_status(job_id, "processing", db, progress=90)
            matches = rfq_service.perform_matching(rfq_items, db)
            rfq_service.save_matches_to_db(matches, db_items, db)

            # Save to CSV
            rfq_service.save_to_csv(rfq_items=rfq_items, rfq_id=rfq_id, matches=matches)

            # Complete
            result = {
                "rfq_id": rfq_id,
                "uploaded_file_id": job.id,
                "rfq_items_count": len(rfq_items),
                "matches_count": len(matches),
                "items": [item.to_dict() for item in db_items],
                "matches": matches,
            }
            self.job_service.update_job_status(
                job_id, "completed", db, progress=100, result=result
            )

        except Exception as e:
            logger.error(f"Error processing RFQ job {job_id}: {e}", exc_info=True)
            self.job_service.update_job_status(
                job_id, "failed", db, error_message=str(e)
            )

    async def process_iatp_job(self, job_id: str, file_path: str, db: Session):
        """
        Process IATP parse and store job

        Args:
            job_id: Job ID
            file_path: Path to Excel file
            db: Database session
        """
        try:
            # Get job to access uploaded_file_id
            job = self.job_service.get_job(job_id, db)

            # Update status to processing
            self.job_service.update_job_status(job_id, "processing", db, progress=10)

            # Parse Excel
            self.job_service.update_job_status(job_id, "processing", db, progress=30)
            products = rfq_service.parse_excel_file(file_path)

            # Save to database with uploaded_file_id
            self.job_service.update_job_status(job_id, "processing", db, progress=70)
            db_products = rfq_service.save_product_master_to_db(products, job.id, db)

            # Save to CSV
            self.job_service.update_job_status(job_id, "processing", db, progress=90)
            rfq_service.save_to_csv(products=products)

            # Reload matching service
            await rfq_service.reload_matching_service_async(db)

            # Complete
            result = {
                "uploaded_file_id": job.id,
                "products_count": len(products),
                "products": [p.to_dict() for p in db_products],
            }
            self.job_service.update_job_status(
                job_id, "completed", db, progress=100, result=result
            )

        except Exception as e:
            logger.error(f"Error processing IATP job {job_id}: {e}", exc_info=True)
            self.job_service.update_job_status(
                job_id, "failed", db, error_message=str(e)
            )

    async def process_competitor_color_job(
        self, job_id: str, file_path: str, db: Session
    ):
        """
        Process competitor color comparison parse and store job

        Args:
            job_id: Job ID
            file_path: Path to PDF file
            db: Database session
        """
        try:
            # Get job to access uploaded_file_id
            job = self.job_service.get_job(job_id, db)

            # Update status to processing
            self.job_service.update_job_status(job_id, "processing", db, progress=10)

            # Parse PDF with pdfplumber (async - runs in thread pool)
            self.job_service.update_job_status(job_id, "processing", db, progress=30)
            parsed_data = await competitor_color_service.parse_pdf(file_path)

            # AI NORMALIZATION: Normalize parsed color data with OpenAI
            self.job_service.update_job_status(job_id, "processing", db, progress=50)

            # Collect all raw rows for normalization
            all_raw_rows = []
            row_metadata = []  # To track which brand each row belongs to

            for brand, items in parsed_data.items():
                for item_data in items:
                    raw_text = item_data.get("raw_text", "")
                    if raw_text:
                        all_raw_rows.append(raw_text)
                        row_metadata.append(
                            {
                                "brand": brand,
                                "item_no": item_data.get("item_no"),
                                "original_data": item_data,
                            }
                        )

            # Normalize with OpenAI if available
            normalized_data = {}
            if all_raw_rows:
                try:
                    from apps.app_nippon_rfq_matching.app.services.openai_normalization import (
                        openai_normalization_service,
                    )

                    logger.info(
                        f"Normalizing {len(all_raw_rows)} competitor color rows with OpenAI"
                    )
                    normalized_rows = openai_normalization_service.normalize_competitor_color_pdf_rows(
                        all_raw_rows
                    )

                    # Rebuild parsed_data with normalized values
                    normalized_data = {}
                    for i, (normalized_row, metadata) in enumerate(
                        zip(normalized_rows, row_metadata)
                    ):
                        brand = metadata["brand"]
                        original_item_no = metadata["item_no"]
                        original_data = metadata["original_data"]

                        if brand not in normalized_data:
                            normalized_data[brand] = []

                        # Use normalized values if available, otherwise use original
                        normalized_data[brand].append(
                            {
                                "item_no": normalized_row.get("item_number")
                                or original_item_no,
                                "source_brand": brand,
                                "source_code": normalized_row.get("source_code")
                                or original_data.get("source_code"),
                                "npms_code": normalized_row.get("npms_code")
                                or original_data.get("npms_code"),
                                "raw_text": normalized_row.get("raw_text")
                                or original_data.get("raw_text"),
                            }
                        )

                    logger.info(
                        f"AI normalization completed for {len(normalized_rows)} rows"
                    )

                except Exception as e:
                    logger.warning(
                        f"AI normalization failed, using original parsed data: {e}"
                    )
                    normalized_data = parsed_data
            else:
                normalized_data = parsed_data

            # Save to database with uploaded_file_id
            self.job_service.update_job_status(job_id, "processing", db, progress=70)
            save_result = competitor_color_service.save_to_database(
                normalized_data, job.id, db
            )

            db_records = save_result["created"]
            duplicates = save_result["duplicates"]
            skipped_count = save_result["skipped_count"]

            # Complete
            # Count by brand dynamically
            brand_counts = {}
            for record in db_records:
                brand = record.source_brand
                brand_counts[brand] = brand_counts.get(brand, 0) + 1

            result = {
                "uploaded_file_id": job.id,
                "created_count": len(db_records),
                "duplicate_count": len(duplicates),
                "skipped_count": skipped_count,
                "brand_counts": brand_counts,
                "records": [r.to_dict() for r in db_records],
                "duplicates": duplicates,
                "ai_normalized": len(all_raw_rows) > 0,
            }
            self.job_service.update_job_status(
                job_id, "completed", db, progress=100, result=result
            )

        except Exception as e:
            logger.error(
                f"Error processing competitor color job {job_id}: {e}", exc_info=True
            )
            self.job_service.update_job_status(
                job_id, "failed", db, error_message=str(e)
            )

    async def process_competitor_color_excel_job(
        self, job_id: str, file_path: str, db: Session
    ):
        """
        Process competitor color comparison Excel parsing job

        Args:
            job_id: Job ID
            file_path: Path to Excel file
            db: Database session
        """
        try:
            # Get job to access uploaded_file_id
            job = self.job_service.get_job(job_id, db)

            # Update status to processing
            self.job_service.update_job_status(job_id, "processing", db, progress=10)

            # Parse Excel file
            self.job_service.update_job_status(job_id, "processing", db, progress=30)
            parsed_data = competitor_color_excel_service.parse_excel_file(file_path)

            # Save to database with uploaded_file_id
            self.job_service.update_job_status(job_id, "processing", db, progress=70)
            save_result = competitor_color_excel_service.save_to_database(
                parsed_data, job.id, db
            )

            db_records = save_result["created"]
            duplicates = save_result["duplicates"]
            skipped_count = save_result["skipped_count"]

            # Count by brand dynamically
            brand_counts = {}
            for record in db_records:
                brand = record.source_brand
                brand_counts[brand] = brand_counts.get(brand, 0) + 1

            result = {
                "uploaded_file_id": job.id,
                "created_count": len(db_records),
                "duplicate_count": len(duplicates),
                "skipped_count": skipped_count,
                "brand_counts": brand_counts,
                "records": [r.to_dict() for r in db_records],
            }
            self.job_service.update_job_status(
                job_id, "completed", db, progress=100, result=result
            )

        except Exception as e:
            logger.error(
                f"Error processing competitor color Excel job {job_id}: {e}",
                exc_info=True,
            )
            self.job_service.update_job_status(
                job_id, "failed", db, error_message=str(e)
            )

    async def process_rfq_parse_only_job(
        self, job_id: str, file_path: str, rfq_id: str, db: Session
    ):
        """
        Process RFQ parse and store job (NO MATCHING - parse and insert only)

        This is a simplified version of process_rfq_job that only:
        1. Parses the PDF/Excel file
        2. Inserts RFQ items into database
        3. Saves to CSV

        Args:
            job_id: Job ID
            file_path: Path to PDF/Excel file
            rfq_id: RFQ identifier
            db: Database session
        """
        from apps.app_nippon_rfq_matching.app.utils.route_excel_upload import (
            PARSER_CERU,
            PARSER_INDONESIA,
            PARSER_ZO_PAINT,
            route_and_parse,
        )

        try:
            # Get job to access uploaded_file_id
            job = self.job_service.get_job(job_id, db)

            # Update status to processing
            self.job_service.update_job_status(job_id, "processing", db, progress=10)

            # Detect file type
            file_ext = file_path.lower().split(".")[-1]
            is_excel = file_ext in ["xlsx", "xls"]
            is_eml = file_ext == "eml"

            # Parse file
            self.job_service.update_job_status(job_id, "processing", db, progress=30)

            rfq_items_data = []
            parser_used = "unknown"

            if is_excel:
                # Use routing utility to auto-detect and parse Excel
                route_result = route_and_parse(file_path)

                if route_result.get("error"):
                    raise ValueError(
                        f"Failed to parse Excel file: {route_result.get('error')}"
                    )

                parser_type = route_result.get("parser_type")
                data = route_result.get("data")

                if parser_type == PARSER_CERU and data:
                    # CERU format: convert to RFQ items
                    for item in data.get("items", []):
                        rfq_items_data.append(
                            {
                                "raw_text": item.get("item_name", ""),
                                "clean_text": item.get("item_name", ""),
                                "qty": str(item.get("qty_reqd", ""))
                                if item.get("qty_reqd")
                                else None,
                                "uom": item.get("unit"),
                                "source": "ceru_excel",
                            }
                        )
                    parser_used = "ceru_excel"

                elif parser_type == PARSER_ZO_PAINT and data:
                    # ZO Paint format: convert to RFQ items
                    for item in data:
                        rfq_items_data.append(
                            {
                                "raw_text": item.get("description", ""),
                                "clean_text": item.get("product_name", ""),
                                "qty": str(item.get("req", ""))
                                if item.get("req")
                                else None,
                                "uom": item.get("unit"),
                                "source": "zo_paint_excel",
                            }
                        )
                    parser_used = "zo_paint_excel"

                elif parser_type == PARSER_INDONESIA and data:
                    # Indonesia format: convert to RFQ items
                    for item in data:
                        description = item.get("Description") or item.get(
                            "description", ""
                        )
                        rfq_items_data.append(
                            {
                                "raw_text": str(description).strip()
                                if description
                                else "",
                                "clean_text": str(description).strip()
                                if description
                                else "",
                                "qty": str(item.get("No", ""))
                                if item.get("No")
                                else None,
                                "uom": None,
                                "source": "indonesia_excel",
                            }
                        )
                    parser_used = "indonesia_excel"

                else:
                    raise ValueError(
                        f"Unsupported Excel format or no data detected. Parser: {parser_type}"
                    )

            elif is_eml:
                # Parse EML file
                rfq_items_data, extracted_rfq_id, email_metadata = (
                    rfq_service.parse_eml_file(file_path)
                )
                parser_used = "eml"
                # Use extracted RFQ ID from EML subject if available
                if extracted_rfq_id:
                    rfq_id = extracted_rfq_id

            else:
                # Parse PDF file
                rfq_items_data = await rfq_service.parse_pdf_file(file_path)
                parser_used = "pdf"

            # Save to database with uploaded_file_id
            self.job_service.update_job_status(job_id, "processing", db, progress=70)
            db_items = rfq_service.save_rfq_items_to_db(
                rfq_items_data, rfq_id, job.id, db
            )

            # Save to CSV (no matching)
            self.job_service.update_job_status(job_id, "processing", db, progress=90)
            rfq_service.save_to_csv(rfq_items=rfq_items_data, rfq_id=rfq_id)

            # Complete - NO MATCHING performed
            result = {
                "rfq_id": rfq_id,
                "uploaded_file_id": job.id,
                "rfq_items_count": len(rfq_items_data),
                "items": [item.to_dict() for item in db_items],
                "parser": parser_used,
                "matching_performed": False,
            }
            self.job_service.update_job_status(
                job_id, "completed", db, progress=100, result=result
            )

            logger.info(
                f"RFQ parse-only job {job_id} completed: {len(rfq_items_data)} items parsed and inserted (no matching)"
            )

        except Exception as e:
            logger.error(
                f"Error processing RFQ parse-only job {job_id}: {e}", exc_info=True
            )
            self.job_service.update_job_status(
                job_id, "failed", db, error_message=str(e)
            )

    async def process_pdf_comparison_export_job(
        self,
        job_id: str,
        rfq_id: str,
        db: Session,
        format_type: str = "table",
        use_normalization: bool = True,
    ):
        """
        Process PDF comparison export job with OpenAI normalization

        Args:
            job_id: Job ID
            rfq_id: RFQ identifier
            db: Database session
            format_type: PDF format type (table, side_by_side, summary)
            use_normalization: Whether to use OpenAI normalization
        """
        try:
            # Update status to processing
            self.job_service.update_job_status(job_id, "processing", db, progress=10)

            # Step 1: Load RFQ items
            self.job_service.update_job_status(job_id, "processing", db, progress=20)
            logger.info(f"Starting PDF comparison export for RFQ: {rfq_id}")

            # Step 2: Normalize with OpenAI (if enabled)
            self.job_service.update_job_status(job_id, "processing", db, progress=40)
            if use_normalization:
                logger.info("Normalizing RFQ items with OpenAI...")
            else:
                logger.info("Skipping OpenAI normalization")

            # Step 3: Find matches and generate PDF
            self.job_service.update_job_status(job_id, "processing", db, progress=60)
            result = pdf_comparison_export_service.export_comparison_report(
                rfq_id=rfq_id,
                db=db,
                format_type=format_type,
                use_normalization=use_normalization,
            )

            # Step 4: Complete
            if result.get("success"):
                self.job_service.update_job_status(
                    job_id, "completed", db, progress=100, result=result
                )
                logger.info(
                    f"PDF comparison export completed: {result.get('pdf_path')}"
                )
            else:
                error_msg = result.get("error", "Unknown error")
                self.job_service.update_job_status(
                    job_id, "failed", db, error_message=error_msg
                )
                logger.error(f"PDF comparison export failed: {error_msg}")

        except Exception as e:
            logger.error(
                f"Error processing PDF comparison export job {job_id}: {e}",
                exc_info=True,
            )
            self.job_service.update_job_status(
                job_id, "failed", db, error_message=str(e)
            )

    async def process_table_results_export_job(
        self,
        job_id: str,
        rfq_id: str,
        db: Session,
        format_type: str = "table",
        use_normalization: bool = True,
        storage_key: str = None,
    ):
        """
        Process table results export job

        Args:
            job_id: Job ID
            rfq_id: RFQ identifier
            db: Database session
            format_type: Result format type (table, detailed, compact)
            use_normalization: Whether to use OpenAI normalization
        """
        try:
            # Update status to processing
            self.job_service.update_job_status(job_id, "processing", db, progress=10)

            # Step 1: Process table result
            self.job_service.update_job_status(job_id, "processing", db, progress=50)
            logger.info(f"Starting table results export for RFQ: {rfq_id}")

            # Run the synchronous processing in a background thread
            result = await run_async_task(
                self._process_table_results_sync_safe,
                rfq_id,
                format_type,
                use_normalization,
                db,
            )

            # Step 2: Save to storage
            self.job_service.update_job_status(job_id, "processing", db, progress=90)

            # Use provided storage_key (should always be provided from the endpoint)
            if not storage_key:
                storage_key = f"table_{job_id[:8]}"  # Fallback if somehow not provided

            # Import here to avoid circular import
            from apps.app_nippon_rfq_matching.app.api.table_results import (
                save_table_result_to_storage,
            )

            saved_file_path = save_table_result_to_storage(result, storage_key)

            # Step 3: Complete
            final_result = {
                "success": True,
                "storage_key": storage_key,
                "result_path": str(saved_file_path),
                "format_type": format_type,
                "normalization_enabled": use_normalization,
                "total_items": result.get("total_items", 0),
                "statistics": result.get("statistics", {}),
            }
            self.job_service.update_job_status(
                job_id, "completed", db, progress=100, result=final_result
            )
            logger.info(f"Table results export completed for RFQ: {rfq_id}")

        except Exception as e:
            logger.error(
                f"Error processing table results export job {job_id}: {e}",
                exc_info=True,
            )
            self.job_service.update_job_status(
                job_id, "failed", db, error_message=str(e)
            )

    def _process_table_results_sync_safe(
        self, rfq_id: str, format_type: str, use_normalization: bool, db: Session
    ):
        """Safely process table results in a background thread"""
        from apps.app_nippon_rfq_matching.app.api.table_results import (
            _process_table_result_sync,
        )

        # Create a new session for this background operation
        from apps.app_nippon_rfq_matching.app.core.database import SessionLocal

        background_db = SessionLocal()

        try:
            return _process_table_result_sync(
                rfq_id, format_type, use_normalization, background_db
            )
        finally:
            background_db.close()

    async def process_quotation_export_job(
        self,
        job_id: str,
        request_data: dict[str, Any],
        rfq_id: str,
        db: Session,
        bearer_token: str = "",
    ):
        """
        Process quotation export job

        Args:
            job_id: Job ID
            request_data: Quotation generation request data
            rfq_id: RFQ identifier
            db: Database session
        """
        try:
            # Update status to processing
            self.job_service.update_job_status(job_id, "processing", db, progress=10)

            # Step 1: Extract items
            self.job_service.update_job_status(job_id, "processing", db, progress=20)

            # Check if using direct items or RFQ ID
            if request_data.get("use_direct_items") and request_data.get("items"):
                logger.info("Using direct items from request data")
                items_data = request_data["items"]
            else:
                logger.info(f"Extracting items from RFQ: {rfq_id}")
                # Get RFQ items from database
                from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem

                rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()
                if not rfq_items:
                    raise ValueError(f"No items found for RFQ: {rfq_id}")

                items_data = []
                for item in rfq_items:
                    item_dict = {
                        "description": item.clean_text or "",
                        "color": item.color or "",
                        "unit": item.uom or "PCS",
                        "quantity": item.qty or 0,
                    }
                    items_data.append(item_dict)

            self.job_service.update_job_status(job_id, "processing", db, progress=40)

            # Step 2: Prepare quotation data
            quotation_data = {
                "rfq_id": rfq_id,
                "client_info": request_data.get("client_info", {}),
                "items": items_data,
                "quotation_number": request_data.get(
                    "quotation_number",
                    f"NPM-Q-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}",
                ),
                "sales_representative": request_data.get(
                    "sales_representative", "Sales Representative"
                ),
                "validity_days": request_data.get("validity_days", 30),
            }

            # Step 3: Generate quotation PDF
            self.job_service.update_job_status(job_id, "processing", db, progress=60)
            logger.info(
                f"Generating quotation PDF with rfq_id: {quotation_data.get('rfq_id')}"
            )

            # Generate quotation in background
            import asyncio

            result = await asyncio.create_task(
                asyncio.to_thread(
                    generate_quotation_background,
                    job_id,
                    db,
                    quotation_data,
                    bearer_token,
                )
            )
            logger.info(f"Quotation PDF generation result: {result}")

            # Step 4: Complete
            if result.get("success"):
                # Store result without overwriting input parameters
                final_result = {
                    "success": True,
                    "pdf_path": result.get("pdf_path"),
                    "quote_number": result.get("quote_number"),
                    "job_id": job_id,
                }
                self.job_service.update_job_status(
                    job_id, "completed", db, progress=100, result=final_result
                )
                logger.info(f"Quotation export completed: {result.get('pdf_path')}")
            else:
                error_msg = result.get("error", "Unknown error")
                self.job_service.update_job_status(
                    job_id, "failed", db, error_message=error_msg
                )
                logger.error(f"Quotation export failed: {error_msg}")

        except Exception as e:
            logger.error(
                f"Error processing quotation export job {job_id}: {e}", exc_info=True
            )
            self.job_service.update_job_status(
                job_id, "failed", db, error_message=str(e)
            )

    async def start_job(self, job_id: str, db: Session = None, **kwargs):
        """
        Start processing a job

        Args:
            job_id: Job ID to process
            db: Database session (optional - will create new session if not provided)
            **kwargs: Additional job-specific parameters (e.g., rfq_id, format_type, use_normalization)
        """
        # Create a new database session for this background task
        # Don't use the request's session as it will be closed
        job_db = get_db_session()

        try:
            job = self.job_service.get_job(job_id, job_db)
            if not job:
                raise ValueError(f"Job {job_id} not found")

            # Merge parameters from job.result if available (stored as parameters)
            if job.result:
                try:
                    import json

                    job_params = json.loads(job.result)
                    logger.info(f"Job {job_id} - Raw result: {job.result}")
                    logger.info(f"Job {job_id} - Parsed params: {job_params}")
                    kwargs.update(job_params)
                except Exception as e:
                    logger.warning(f"Failed to parse job parameters for {job_id}: {e}")

            if job.job_type == "rfq_parse_store":
                # Extract rfq_id from file path or use job_id
                rfq_id = f"rfq_{job_id[:8]}"
                await self.process_rfq_job(job_id, job.file_path, rfq_id, job_db)
            elif job.job_type == "rfq_parse_only":
                # Extract rfq_id from kwargs or use job_id
                rfq_id = kwargs.get("rfq_id", f"rfq_{job_id[:8]}")
                await self.process_rfq_parse_only_job(
                    job_id, job.file_path, rfq_id, job_db
                )
            elif job.job_type == "iatp_parse_store":
                await self.process_iatp_job(job_id, job.file_path, job_db)
            elif job.job_type == "competitor_color_parse":
                await self.process_competitor_color_job(job_id, job.file_path, job_db)
            elif job.job_type == "competitor_color_excel_parse":
                await self.process_competitor_color_excel_job(
                    job_id, job.file_path, job_db
                )
            elif job.job_type == "pdf_comparison_export":
                # Get job parameters
                rfq_id = kwargs.get("rfq_id")
                format_type = kwargs.get("format_type", "table")
                use_normalization = kwargs.get("use_normalization", True)
                await self.process_pdf_comparison_export_job(
                    job_id, rfq_id, job_db, format_type, use_normalization
                )
            elif job.job_type == "quotation_export":
                # Get job parameters (already merged into kwargs at the top)
                request_data = kwargs.get("request_data", {})
                rfq_id = kwargs.get("rfq_id")
                logger.info(f"Quotation export job - request_data: {request_data}")
                logger.info(f"Quotation export job - rfq_id from kwargs: {rfq_id}")
                logger.info(f"Quotation export job - job.result: {job.result}")
                bearer_token = request_data.get("bearer_token", "")
                await self.process_quotation_export_job(
                    job_id, request_data, rfq_id, job_db, bearer_token
                )
            elif job.job_type == "table_results_export":
                # Get job parameters
                rfq_id = kwargs.get("rfq_id")
                format_type = kwargs.get("format_type", "table")
                use_normalization = kwargs.get("use_normalization", True)
                storage_key = kwargs.get("storage_key")
                await self.process_table_results_export_job(
                    job_id, rfq_id, job_db, format_type, use_normalization, storage_key
                )
            else:
                raise ValueError(f"Unknown job type: {job.job_type}")

        finally:
            # Always close the session
            job_db.close()

    async def start_job_safe(self, job_id: str, **kwargs):
        """
        Start processing a job with error handling

        This wrapper ensures errors are caught and logged without crashing the background task.

        Args:
            job_id: Job ID to process
            **kwargs: Additional job-specific parameters
        """
        try:
            await self.start_job(job_id, **kwargs)
        except Exception as e:
            logger.error(f"Error in background job {job_id}: {e}", exc_info=True)

            # Try to update job status to failed
            try:
                job_db = get_db_session()
                try:
                    self.job_service.update_job_status(
                        job_id, "failed", job_db, error_message=str(e)
                    )
                finally:
                    job_db.close()
            except Exception as e2:
                logger.error(
                    f"Failed to update job status to failed: {e2}", exc_info=True
                )


# Singleton instances
job_service = JobService()
job_processor = JobProcessor(job_service)


def run_job_background(job_id: str, **kwargs):
    """
    Run a job in background using the sequential queue.

    Jobs are processed SEQUENTIALLY in a queue (one at a time).

    Args:
        job_id: Job ID to process
        **kwargs: Additional job parameters (rfq_id, format_type, etc.)
    """
    # Add job to sequential queue with parameters
    sequential_job_queue.add_job(job_id, **kwargs)


def run_pdf_comparison_export_job(
    job_id: str, rfq_id: str, format_type: str = "table", use_normalization: bool = True
):
    """
    Run a PDF comparison export job in background with parameters

    Jobs are processed SEQUENTIALLY in a queue (one at a time).

    Args:
        job_id: Job ID to process
        rfq_id: RFQ identifier
        format_type: PDF format type (table, side_by_side, summary)
        use_normalization: Whether to use OpenAI normalization
    """
    # Add job to sequential queue
    sequential_job_queue.add_job(
        job_id,
        rfq_id=rfq_id,
        format_type=format_type,
        use_normalization=use_normalization,
    )


def run_rfq_parse_only_job(job_id: str, rfq_id: str):
    """
    Run an RFQ parse-only job in background with rfq_id parameter

    This job only parses and inserts RFQ data, without performing matching.
    Jobs are processed SEQUENTIALLY in a queue (one at a time).

    Args:
        job_id: Job ID to process
        rfq_id: RFQ identifier
    """
    # Add job to sequential queue instead of spawning a new thread
    sequential_job_queue.add_job(job_id, rfq_id=rfq_id)


def run_table_results_export_job(
    job_id: str,
    rfq_id: str,
    format_type: str = "table",
    use_normalization: bool = True,
    storage_key: str = None,
):
    """
    Run a table results export job in background with parameters

    Jobs are processed SEQUENTIALLY in a queue (one at a time).

    Args:
        job_id: Job ID to process
        rfq_id: RFQ identifier
        format_type: Result format type (table, detailed, compact)
        use_normalization: Whether to use OpenAI normalization
        storage_key: Storage key for the result file
    """
    # Add job to sequential queue with storage_key
    sequential_job_queue.add_job(
        job_id,
        rfq_id=rfq_id,
        format_type=format_type,
        use_normalization=use_normalization,
        storage_key=storage_key,
    )
