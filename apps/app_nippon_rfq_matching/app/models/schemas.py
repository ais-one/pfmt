"""
Pydantic schemas for API requests and responses
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_serializer, field_validator


# Product Master Schemas
class ProductMasterBase(BaseModel):
    """Base product master schema"""

    sheet_name: str | None = None
    sheet_type: str
    row_excel: int | None = None
    pmc: str
    product_name: str
    color: str | None = None
    clean_product_name: str | None = None


class ProductMasterCreate(ProductMasterBase):
    """Schema for creating product master"""

    pass


class ProductMasterResponse(ProductMasterBase):
    """Schema for product master response"""

    id: int
    created_at: str | None = None

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True


# RFQ Item Schemas
class RFQItemBase(BaseModel):
    """Base RFQ item schema"""

    rfq_id: str
    raw_text: str
    clean_text: str | None = None
    qty: str | None = None
    uom: str | None = None
    source: str


class RFQItemCreate(RFQItemBase):
    """Schema for creating RFQ item"""

    pass


class RFQItemResponse(RFQItemBase):
    """Schema for RFQ item response"""

    id: int
    created_at: str | None = None

    class Config:
        from_attributes = True


# RFQ Match Schemas
class RFQMatchBase(BaseModel):
    """Base RFQ match schema"""

    rfq_item_id: int
    product_master_id: int
    matched_text: str
    score: float
    method: str  # 'fuzzy' or 'cosine'


class RFQMatchCreate(RFQMatchBase):
    """Schema for creating RFQ match"""

    pass


class RFQMatchResponse(RFQMatchBase):
    """Schema for RFQ match response"""

    id: int
    created_at: str | None = None

    class Config:
        from_attributes = True


# Match Result Schema (for API responses)
class RFQItemMatch(BaseModel):
    """RFQ item data"""

    raw_text: str
    clean_text: str
    qty: str | None = None
    uom: str | None = None
    source: str


class ProductMasterMatch(BaseModel):
    """Product master data"""

    id: int | None = None
    clean_product_name: str | None = None
    pmc: str | None = None
    product_name: str | None = None
    color: str | None = None
    sheet_type: str | None = None


class MatchInfo(BaseModel):
    """Match information"""

    score: float
    method: str
    extracted_color: str | None = None
    color_match: bool = False


class MatchResult(BaseModel):
    """Schema for match result with structured data"""

    rfq: RFQItemMatch
    product_master: ProductMasterMatch
    match_info: MatchInfo


# Enterprise Response Schemas
class APIResponse(BaseModel):
    """Standard API Response format"""

    success: bool
    message: str
    data: Any | None = None
    meta: dict[str, Any] | None = None
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class APIErrorResponse(APIResponse):
    """Standard API Error Response format"""

    success: bool = False
    error: str | None = None
    error_code: str | None = None
    details: dict[str, Any] | None = None
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class PaginatedResponse(APIResponse):
    """Standard Paginated API Response format"""

    success: bool = True
    data: list[Any]
    meta: dict[str, Any] = Field(default_factory=dict)

    @field_validator("meta")
    @classmethod
    def validate_meta(cls, v):
        required_fields = {
            "page",
            "page_size",
            "total",
            "total_pages",
            "has_next",
            "has_prev",
        }
        for field in required_fields:
            if field not in v:
                v[field] = None
        return v


# Upload Response Schemas
class ExcelUploadResponse(APIResponse):
    """Response for Excel upload"""

    success: bool = True
    data: dict[str, Any] | None = None
    meta: dict[str, Any] | None = None


class PDFUploadResponse(BaseModel):
    """Response for PDF upload"""

    status: str
    rfq_id: str
    uploaded_file_id: int
    rfq_items_count: int
    rfq_items: list[RFQItemResponse]
    matches: list[MatchResult]


# Query Schemas
class ProductMasterQueryResponse(BaseModel):
    """Response for product master query"""

    data: list[ProductMasterResponse]
    count: int
    page: int
    page_size: int


class ProductMasterDetailResponse(BaseModel):
    """Response for product master detail with pricing"""

    id: int
    sheet_name: str | None = None
    sheet_type: str
    row_excel: int | None = None
    pmc: str
    product_name: str
    color: str | None = None
    clean_product_name: str | None = None
    created_at: str | None = None
    regions: list[dict[str, Any]] = []

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True


class RegionPriceResponse(BaseModel):
    """Response for region price data"""

    region_id: int
    region_name: str
    size: float | None = None
    uom: str | None = None
    price: float | None = None
    price_raw: str | None = None
    created_at: str | None = None


class ProductMasterWithRegionsResponse(BaseModel):
    """Response for product master with paginated regions"""

    product_master: ProductMasterDetailResponse
    total_regions: int
    regions_count: int
    page: int
    page_size: int
    has_next: bool
    has_prev: bool
    regions: list[RegionPriceResponse]


class RFQItemsQueryResponse(BaseModel):
    """Response for RFQ items query"""

    data: list[RFQItemResponse]
    count: int
    rfq_id: str | None = None


class RFQMatchesQueryResponse(BaseModel):
    """Response for RFQ matches query"""

    data: list[MatchResult]
    count: int
    rfq_id: str | None = None


# CSV Summary Schema
class CSVSummaryResponse(BaseModel):
    """Response for CSV storage summary"""

    product_master: dict
    rfq_items: dict
    rfq_matches: dict


# Top Match Schema
class TopMatchResult(BaseModel):
    """Schema for top match result"""

    matched: str
    score: float
    method: str
    index: int


class TopMatchesResponse(BaseModel):
    """Response for top matches query"""

    query: str
    matches: list[TopMatchResult]


# Dataframe Query Schema
class DataframeQueryResponse(BaseModel):
    """Response for dataframe query"""

    rfq_id: str | None = None
    data: list[dict]
    columns: list[str]
    shape: list[int]  # [rows, cols]


# Job Schemas
class JobUploadResponse(BaseModel):
    """Response for file upload that creates a job"""

    status: str
    job_id: str
    job_type: str
    message: str = "Job created successfully. Use the job_id to check status."


class JobStatusResponse(BaseModel):
    """Response for job status query"""

    job_id: str
    job_type: str
    status: str  # pending, processing, completed, failed
    progress: int  # 0-100
    result_data: dict | None = None
    error_message: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None


# Files API Schemas
class UploadedFileStats(BaseModel):
    """Statistics for uploaded file"""

    rfq_items_count: int | None = None
    rfq_ids: list[str] | None = None
    products_count: int | None = None
    matched_rfqs: list[str] | None = None
    matched_rfqs_count: int | None = None


class UploadedFileResponse(BaseModel):
    """Response for uploaded file"""

    id: int
    original_filename: str
    stored_filename: str
    file_type: str  # 'excel' or 'pdf'
    status: str  # pending, parsed, error
    error_message: str | None = None
    created_at: str | None = None
    stats: UploadedFileStats | None = None


class RFQItemsWithMatchesResponse(BaseModel):
    """Response for RFQ items with their matches"""

    rfq_id: str
    total_items: int
    results: list[dict]


class ProductMasterUsageResponse(BaseModel):
    """Response for Product Master usage statistics"""

    file_id: int
    filename: str
    products_count: int
    matched_rfqs: list[str]
    total_rfqs_using: int
    status: str


# PDF Generation Schemas
class PDFGenerateRequest(BaseModel):
    """Request schema for PDF generation"""

    rfq_id: str = Field(..., description="RFQ ID to generate report for")
    format_type: str = Field(
        default="table", description="PDF format type: table, side_by_side, or summary"
    )
    min_score: float | None = Field(
        default=70.0, ge=0, le=100, description="Minimum match score to include"
    )
    include_competitor: bool = Field(
        default=False,
        description="Include competitor product matching results using multi-keyword search",
    )
    max_results: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum results per RFQ item for competitor matching",
    )


class PDFFormatInfo(BaseModel):
    """Information about available PDF formats"""

    format_type: str
    name: str
    description: str


class PDFFormatsResponse(BaseModel):
    """Response for available PDF formats"""

    formats: list[PDFFormatInfo]
    default_format: str


class PDFConfigSchema(BaseModel):
    """Schema for PDF configuration"""

    title: str | None = None
    company_name: str | None = "Nippon Paint Marine"
    page_size: str = "A4"
    margin: float = 0.75
    show_page_numbers: bool = True
    show_timestamp: bool = True
    primary_color: str = "#1a365d"
    header_bg_color: str = "#2c5282"
    alternate_row_color: str = "#ebf8ff"


# Competitor Matrix Schemas
class GenericBase(BaseModel):
    """Base generic schema"""

    name: str


class GenericCreate(GenericBase):
    """Schema for creating generic"""

    pass


class GenericResponse(GenericBase):
    """Schema for generic response"""

    id: int
    created_at: datetime | None = None

    @field_serializer("created_at")
    def serialize_created_at(self, dt: datetime | None) -> str | None:
        """Serialize datetime to ISO string"""
        return dt.isoformat() if dt else None

    class Config:
        from_attributes = True


class BrandBase(BaseModel):
    """Base brand schema"""

    name: str


class BrandCreate(BrandBase):
    """Schema for creating brand"""

    pass


class BrandResponse(BrandBase):
    """Schema for brand response"""

    id: int
    created_at: datetime | None = None

    @field_serializer("created_at")
    def serialize_created_at(self, dt: datetime | None) -> str | None:
        """Serialize datetime to ISO string"""
        return dt.isoformat() if dt else None

    class Config:
        from_attributes = True


class CompetitorProductBase(BaseModel):
    """Base competitor product schema"""

    brand_id: int
    name: str
    description: str | None = None


class CompetitorProductCreate(CompetitorProductBase):
    """Schema for creating competitor product"""

    pass


class CompetitorProductResponse(CompetitorProductBase):
    """Schema for competitor product response"""

    id: int
    created_at: datetime | None = None
    brand: BrandResponse | None = None

    @field_serializer("created_at")
    def serialize_created_at(self, dt: datetime | None) -> str | None:
        """Serialize datetime to ISO string"""
        return dt.isoformat() if dt else None

    class Config:
        from_attributes = True


class ProductEquivalentBase(BaseModel):
    """Base product equivalent schema - direct competitor to Nippon mapping"""

    competitor_product_id: int
    nippon_product_name: str


class ProductEquivalentCreate(ProductEquivalentBase):
    """Schema for creating product equivalent"""

    pass


class ProductEquivalentResponse(ProductEquivalentBase):
    """Schema for product equivalent response"""

    id: int
    created_at: datetime | None = None
    competitor_product: CompetitorProductResponse | None = None

    @field_serializer("created_at")
    def serialize_created_at(self, dt: datetime | None) -> str | None:
        """Serialize datetime to ISO string"""
        return dt.isoformat() if dt else None

    class Config:
        from_attributes = True


class CompetitorMatrixUploadResponse(BaseModel):
    """Response for competitor matrix upload"""

    status: str
    generics_count: int
    brands_count: int
    products_count: int
    equivalents_count: int
    generics: list[GenericResponse]
    brands: list[BrandResponse]


class GenericWithProductsResponse(GenericResponse):
    """Schema for generic with its equivalent products"""

    products: list[CompetitorProductResponse]


class BrandWithProductsResponse(BrandResponse):
    """Schema for brand with its products"""

    products: list[CompetitorProductResponse]


# Competitor Color Comparison Schemas
class CompetitorColorComparisonBase(BaseModel):
    """Base competitor color comparison schema"""

    item_no: int
    source_brand: str  # "JOTUN" or "INTERNATIONAL"
    source_code: str
    npms_code: str | None = None
    raw_text: str | None = None


class CompetitorColorComparisonCreate(CompetitorColorComparisonBase):
    """Schema for creating competitor color comparison"""

    pass


class CompetitorColorComparisonResponse(CompetitorColorComparisonBase):
    """Schema for competitor color comparison response"""

    id: int
    uploaded_file_id: int | None = None
    created_at: datetime | None = None

    @field_serializer("created_at")
    def serialize_created_at(self, dt: datetime | None) -> str | None:
        """Serialize datetime to ISO string"""
        return dt.isoformat() if dt else None

    class Config:
        from_attributes = True


class CompetitorColorUploadResponse(BaseModel):
    """Response for competitor color comparison upload"""

    status: str
    job_id: str
    job_type: str
    message: str = "Competitor color comparison PDF uploaded successfully. Use the job_id to check status."


class CompetitorColorJobStatusResponse(BaseModel):
    """Response for competitor color comparison job status"""

    job_id: str
    job_type: str
    status: str  # pending, processing, completed, failed
    progress: int  # 0-100
    result_data: dict | None = None
    error_message: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None


# Deprecated: Use PaginatedResponse instead
class CompetitorColorComparisonListResponse(BaseModel):
    """Response for listing competitor color comparison data"""

    data: list[CompetitorColorComparisonResponse]
    count: int
    source_brand: str | None = None


# RFQ Parse Only Schemas (background job - parse and insert only, no matching)
class RFQParseOnlyUploadResponse(BaseModel):
    """Response for RFQ parse-only upload (background job)"""

    status: str
    job_id: str
    job_type: str
    message: str = "RFQ file uploaded successfully. Parse and insert job created. Use the job_id to check status."


class RFQParseOnlyJobStatusResponse(BaseModel):
    """Response for RFQ parse-only job status"""

    job_id: str
    job_type: str
    status: str  # pending, processing, completed, failed
    progress: int  # 0-100
    result_data: dict | None = None
    error_message: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None


# Normalization Cache Schemas
class NormalizationCacheResponse(BaseModel):
    """Schema for normalization cache entry"""

    id: int
    raw_text: str
    normalized_text: str | None = None
    product_type: str | None = None
    match_confidence: float = 1.0
    times_used: int = 1
    last_used_at: str | None = None
    created_at: str | None = None

    class Config:
        from_attributes = True


class CacheStatsResponse(BaseModel):
    """Schema for cache statistics"""

    total_entries: int
    nippon_entries: int
    competitor_entries: int
    no_match_entries: int
    most_used: list[NormalizationCacheResponse]
