# RFQ Product Matching API

FastAPI-based system for parsing RFQ (Request for Quotation) files and matching products using openAI normalization format.

## Features

- **Excel Parsing**: Parse IATP Excel files (AF, SW, GEN sheet types)
- **PDF Parsing**: Parse RFQ PDF files using Docling
- **Product Matching**: Hybrid matching using fuzzy matching (RapidFuzz) and TF-IDF cosine similarity (scikit-learn)
- **CSV Storage**: Store normalized data in CSV format for easy querying
- **Database**: SQLAlchemy-based SQLite database for structured storage
- **API Endpoints**: RESTful API for upload, parsing, and querying

## Project Structure

```
rfq_product_matching_nippon_fastapi/
├── app/
│   ├── api/                    # API endpoints
│   │   ├── upload.py           # File upload endpoints
│   │   └── query.py            # Data query endpoints
│   ├── core/                   # Core configuration
│   │   ├── config.py           # Application settings
│   │   └── database.py         # Database session management
│   ├── models/                 # Data models
│   │   ├── database.py         # SQLAlchemy models
│   │   └── schemas.py          # Pydantic schemas
│   ├── services/               # Business logic
│   │   ├── matching.py         # Matching service (fuzzy + cosine)
│   │   └── rfq_service.py      # RFQ processing service
│   ├── utils/                  # Utilities
│   │   ├── parsers.py          # Excel/PDF parsers
│   │   └── csv_storage.py      # CSV storage handler
│   └── main.py                 # FastAPI application
├── data/                       # Data storage
│   ├── uploads/                # Uploaded files
│   └── storage/                # Processed data
│       ├── csv/                # CSV exports
│       └── models/             # Saved ML models
├── requirements.txt            # Python dependencies
└── .env                        # Environment variables
```

## Database Schema

### ProductMaster
- `id`: Primary key
- `sheet_name`: Excel sheet name
- `sheet_type`: IATP_AF, IATP_SW, or IATP_GEN
- `row_excel`: Row number in Excel
- `pmc`: Product code
- `product_name`: Product name
- `color`: Product color
- `clean_product_name`: Cleaned product name for matching

### RFQItem
- `id`: Primary key
- `rfq_id`: RFQ identifier
- `raw_text`: Original text from PDF
- `clean_text`: Cleaned text for matching
- `qty`: Quantity
- `uom`: Unit of measure
- `source`: Source (rfq_1_table_0, rfq_1_table_1, rfq_2)

### RFQMatch
- `id`: Primary key
- `rfq_item_id`: Foreign key to RFQItem
- `product_master_id`: Foreign key to ProductMaster
- `matched_text`: Matched text
- `score`: Match score (0-100)
- `method`: Matching method (fuzzy or cosine)

## Installation

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Running the Application

### Development
```bash
python -m app.main
```

### Production
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Endpoints

### Upload Endpoints

#### Upload Excel File
```bash
POST /api/v1/upload/excel
Content-Type: multipart/form-data

curl -X POST "http://localhost:8000/api/v1/upload/excel" \
  -F "file=@/path/to/iatp_file.xlsx"
```

#### Upload PDF File
```bash
POST /api/v1/upload/pdf
Content-Type: multipart/form-data

curl -X POST "http://localhost:8000/api/v1/upload/pdf" \
  -F "file=@/path/to/rfq_file.pdf" \
  -F "rfq_id=RFQ-12345"
```

### Query Endpoints

#### Query Product Master
```bash
GET /api/v1/query/product-master?page=1&page_size=50&sheet_type=IATP_AF&search=search_term
```

#### Query RFQ Items
```bash
GET /api/v1/query/rfq-items?rfq_id=RFQ-12345&source=rfq_1_table_1
```

#### Query RFQ Matches
```bash
GET /api/v1/query/rfq-matches?rfq_id=RFQ-12345&min_score=70&method=fuzzy
```

#### Get Dataframe (JSON/CSV)
```bash
GET /api/v1/query/dataframe/product-master?format=json
GET /api/v1/query/dataframe/rfq-items/{rfq_id}?format=csv
```

#### Get Top Matches
```bash
GET /api/v1/query/match/top?query=search_term&top_n=5
```

#### CSV Storage Summary
```bash
GET /api/v1/query/csv/summary
```

## CSV Storage

Data is automatically saved to CSV files in `data/storage/csv/`:
- `product_master.csv`: All product master data
- `rfq_items.csv`: All RFQ items with RFQ ID
- `rfq_matches.csv`: All match results

## Matching Algorithm

The system uses a hybrid matching approach:

1. **Fuzzy Matching**: Uses RapidFuzz with token set ratio for flexible string matching
2. **TF-IDF Cosine Similarity**: Uses scikit-learn for semantic matching
3. **Hybrid**: Selects the best score between fuzzy and cosine methods

### Text Cleaning
- Convert to uppercase
- Remove brackets and content within
- Remove UOM annotations
- Remove special characters
- Normalize whitespace

## Development

### Running Tests
```bash
pytest tests/
```

### API Documentation
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## License

MIT License
