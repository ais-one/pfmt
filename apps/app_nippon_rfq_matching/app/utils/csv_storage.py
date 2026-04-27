"""
CSV storage utility for normalized data
"""

from datetime import datetime
from typing import Any

import pandas as pd

from apps.app_nippon_rfq_matching.app.core.config import settings


class CSVStorage:
    """Handle CSV storage for normalized data"""

    def __init__(self):
        """Initialize CSV storage directories"""
        self.csv_dir = settings.CSV_DIR
        self.csv_dir.mkdir(parents=True, exist_ok=True)

        # Define CSV file paths
        self.product_master_csv = self.csv_dir / "product_master.csv"
        self.rfq_items_csv = self.csv_dir / "rfq_items.csv"
        self.rfq_matches_csv = self.csv_dir / "rfq_matches.csv"

    def save_product_master(self, data: list[dict[str, Any]]) -> str:
        """
        Save product master data to CSV

        Args:
            data: List of product master records

        Returns:
            Path to saved CSV file
        """
        df = pd.DataFrame(data)

        # Add timestamp
        df["exported_at"] = datetime.utcnow().isoformat()

        # Save to CSV
        df.to_csv(self.product_master_csv, index=False)

        return str(self.product_master_csv)

    def save_rfq_items(self, data: list[dict[str, Any]], rfq_id: str) -> str:
        """
        Save RFQ items to CSV

        Args:
            data: List of RFQ item records
            rfq_id: RFQ identifier

        Returns:
            Path to saved CSV file
        """
        df = pd.DataFrame(data)

        # Add RFQ ID and timestamp
        df["rfq_id"] = rfq_id
        df["exported_at"] = datetime.utcnow().isoformat()

        # Append to CSV or create new
        if self.rfq_items_csv.exists():
            existing_df = pd.read_csv(self.rfq_items_csv)
            df = pd.concat([existing_df, df], ignore_index=True)

        df.to_csv(self.rfq_items_csv, index=False)

        return str(self.rfq_items_csv)

    def save_rfq_matches(self, data: list[dict[str, Any]]) -> str:
        """
        Save RFQ match results to CSV (handles structured format)

        Args:
            data: List of RFQ match records (structured or flat format)

        Returns:
            Path to saved CSV file
        """
        # Flatten structured format if needed
        flattened_data = []
        for match in data:
            if "rfq" in match and "product_master" in match and "match_info" in match:
                # Structured format - flatten it
                flat = {
                    "rfq_raw_text": match["rfq"].get("raw_text", ""),
                    "rfq_clean_text": match["rfq"].get("clean_text", ""),
                    "rfq_qty": match["rfq"].get("qty", ""),
                    "rfq_uom": match["rfq"].get("uom", ""),
                    "rfq_source": match["rfq"].get("source", ""),
                    "product_id": match["product_master"].get("id", ""),
                    "product_pmc": match["product_master"].get("pmc", ""),
                    "product_name": match["product_master"].get("product_name", ""),
                    "product_color": match["product_master"].get("color", ""),
                    "match_score": match["match_info"].get("score", 0),
                    "match_method": match["match_info"].get("method", ""),
                    "extracted_color": match["match_info"].get("extracted_color", ""),
                    "color_match": match["match_info"].get("color_match", False),
                }
                flattened_data.append(flat)
            else:
                # Already flat format
                flattened_data.append(match)

        df = pd.DataFrame(flattened_data)

        # Add timestamp
        df["exported_at"] = datetime.utcnow().isoformat()

        # Append to CSV or create new
        if self.rfq_matches_csv.exists():
            existing_df = pd.read_csv(self.rfq_matches_csv)
            df = pd.concat([existing_df, df], ignore_index=True)

        df.to_csv(self.rfq_matches_csv, index=False)

        return str(self.rfq_matches_csv)

    def load_product_master(self) -> pd.DataFrame:
        """
        Load product master from CSV

        Returns:
            DataFrame with product master data
        """
        if self.product_master_csv.exists():
            return pd.read_csv(self.product_master_csv)
        return pd.DataFrame()

    def load_rfq_items(self, rfq_id: str | None = None) -> pd.DataFrame:
        """
        Load RFQ items from CSV

        Args:
            rfq_id: Optional RFQ ID to filter

        Returns:
            DataFrame with RFQ items
        """
        if self.rfq_items_csv.exists():
            df = pd.read_csv(self.rfq_items_csv)
            if rfq_id:
                df = df[df["rfq_id"] == rfq_id]
            return df
        return pd.DataFrame()

    def load_rfq_matches(self, rfq_id: str | None = None) -> pd.DataFrame:
        """
        Load RFQ matches from CSV

        Args:
            rfq_id: Optional RFQ ID to filter

        Returns:
            DataFrame with RFQ matches
        """
        if self.rfq_matches_csv.exists():
            df = pd.read_csv(self.rfq_matches_csv)
            if rfq_id:
                df = df[df["rfq_id"] == rfq_id]
            return df
        return pd.DataFrame()

    def get_summary(self) -> dict[str, Any]:
        """
        Get summary of stored data

        Returns:
            Dictionary with summary statistics
        """
        summary = {
            "product_master": {
                "file_exists": self.product_master_csv.exists(),
                "record_count": 0,
            },
            "rfq_items": {
                "file_exists": self.rfq_items_csv.exists(),
                "record_count": 0,
            },
            "rfq_matches": {
                "file_exists": self.rfq_matches_csv.exists(),
                "record_count": 0,
            },
        }

        if self.product_master_csv.exists():
            df = pd.read_csv(self.product_master_csv)
            summary["product_master"]["record_count"] = len(df)

        if self.rfq_items_csv.exists():
            df = pd.read_csv(self.rfq_items_csv)
            summary["rfq_items"]["record_count"] = len(df)

        if self.rfq_matches_csv.exists():
            df = pd.read_csv(self.rfq_matches_csv)
            summary["rfq_matches"]["record_count"] = len(df)

        return summary


# Singleton instance
csv_storage = CSVStorage()
