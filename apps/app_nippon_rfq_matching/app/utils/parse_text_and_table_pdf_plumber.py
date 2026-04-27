def extract_vendor_from_text(text: str) -> list[str]:
    # Placeholder: vendor extraction from raw page text is not yet implemented.
    return []


def parse_pdf(path):
    results = {"vendors": [], "tables": []}
    import pandas as pd
    import pdfplumber

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            # TEXT
            text = page.extract_text() or ""
            results["vendors"].extend(extract_vendor_from_text(text))

            # TABLE
            tables = page.extract_tables()
            for table in tables:
                results["tables"].append(pd.DataFrame(table))

    return results
