"""
src/food_scanner/data/data_backup_manager.py
Responsibility: Local backup of data (Supabase free tier)
Shared by the whole ETL pipeline
- No business logic
- Just persistence of data
- Always optional (never breaks the pipeline)
"""

import json
from pathlib import Path
from typing import Dict, Any, List
from dataclasses import dataclass



class DataBackupManager:
    """
    RESPONSIBILITY : Local backup of data for the whole ETL pipeline (Supabase free tier)
    - No business logic
    - Just persistence of data
    - Always optional (never breaks the pipeline)
    """
    
    def __init__(self, backup_dir: Path):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Backup files
        self.products_backup = self.backup_dir / "products_backup.json"
        self.extraction_history = self.backup_dir / "extraction_history.json"
        self.rejected_products_file = self.backup_dir / "rejected_products_for_retry.json"
    
    def save_extraction_batch(self, extracted_products: Dict[str, Any],timestamp: str) -> bool:
        """Save an extraction batch (always succeeds)"""
        try:
            # Load existing history
            history = self._load_extraction_history()
            
            # Add this batch to the history
            batch_id = f"extraction_{timestamp.replace(':', '_')}"
            history[batch_id] = {
                "timestamp": timestamp,
                "product_count": len(extracted_products),
                "products": extracted_products
            }
            
            # Save
            with open(self.extraction_history, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=2, default=str, ensure_ascii=False)
            
            print(f"      📁 Backup saved: {len(extracted_products)} products")
            return True
            
        except Exception as e:
            print(f"      ⚠️ Backup failed (non-critical): {e}")
            return False  # Does not affect the pipeline

    def save_rejected_products_for_retry(self, rejected_products: Dict[str, Any], timestamp: str) -> bool:
        """Save rejected products for retry (for next extraction) + manual verification"""
        try:
            retry_data = {
                "timestamp": timestamp,
                "rejected_barcodes": list(rejected_products.keys()),
                "rejection_details": rejected_products
            }
            
            with open(self.rejected_products_file, 'w', encoding='utf-8') as f:
                json.dump(retry_data, f, indent=2, default=str, ensure_ascii=False)
            
            print(f"      📋 Rejected products saved for retry: {len(rejected_products)}")
            return True
            
        except Exception as e:
            print(f"      ⚠️ Could not save rejected products: {e}")
            return False
    
    def get_rejected_products_for_retry_backup(self) -> List[str]:
        """Get the list of barcodes to retry (for next extraction)"""
        try:
            if self.rejected_products_file.exists():
                with open(self.rejected_products_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("rejected_barcodes", [])
        except Exception as e:
            print(f"      ⚠️ Could not load rejected products: {e}")
        return []

    def get_all_products(self) -> Dict[str, Any]: # TODO: decide to use this method or remove it
        """Get all saved products (for analysis)"""
        try:
            history = self._load_extraction_history()
            all_products = {}
            
            for batch_id, batch_data in history.items():
                products = batch_data.get("products", {})
                all_products.update(products)
            
            return all_products
            
        except Exception as e:
            print(f"      ⚠️ Could not load backup: {e}")
            return {}
    
    def _load_extraction_history(self) -> Dict[str, Any]: # TODO: decide to use this method or remove it
        """Load the extraction history"""
        if self.extraction_history.exists():
            try:
                with open(self.extraction_history, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

