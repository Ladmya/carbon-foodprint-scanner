"""
    RESPONSIBILITY: Perform final quality checks on products ready for database
    
    BUSINESS LOGIC:
    1. Validate database readiness of products
    2. Check for critical field completeness
    3. Perform final data integrity checks
    4. Generate quality validation report
"""
from typing import Dict, Any, List

class QualityChecker:
    """
    RESPONSIBILITY: Perform final quality checks on products ready for database
    
    BUSINESS LOGIC:
    1. Validate database readiness of products
    2. Check for critical field completeness
    3. Perform final data integrity checks
    4. Generate quality validation report
    """
    
    def __init__(self):
        self.quality_stats = {
            "products_checked": 0,
            "database_ready_products": 0,
            "quality_issues_found": 0,
            "integrity_checks_performed": 0
        }
        
        self.quality_issues = []
    
    def perform_final_quality_checks(self, products_with_metadata: Dict[str, Any]) -> Dict[str, Any]: # TODO: add other critical fields
        """
        Perform final quality checks on products before database insertion
        
        Args:
            products_with_metadata: Products with complete metadata
            
        Returns:
            Quality-checked products with validation results
        """
        quality_checked_products = {}
        
        for barcode, product_data in products_with_metadata.items():
            transformed_data = product_data["transformed_data"]
            
            # Perform quality checks
            quality_result = self._check_database_readiness(transformed_data, barcode)
            
            # Add quality validation to product data
            product_data["quality_validation"] = quality_result
            
            # Only include products that pass quality checks
            if quality_result["database_ready"]:
                quality_checked_products[barcode] = product_data
                self.quality_stats["database_ready_products"] += 1
            else:
                self.quality_issues.extend(quality_result["issues"])
                self.quality_stats["quality_issues_found"] += len(quality_result["issues"])
            
            self.quality_stats["products_checked"] += 1
            self.quality_stats["integrity_checks_performed"] += 1
        
        return quality_checked_products
    
    def _check_database_readiness(self, transformed_data: Dict[str, Any], barcode: str) -> Dict[str, Any]:
        """
        Check if product is ready for database insertion
        
        Args:
            transformed_data: Product's transformed data
            barcode: Product barcode
            
        Returns:
            Quality check results
        """
        issues = []
        
        # Check critical fields for database
        critical_fields = ["barcode", "product_name", "brand_name", "created_at"]
        for field in critical_fields:
            if not transformed_data.get(field):
                issues.append({
                    "type": "missing_critical_field",
                    "field": field,
                    "barcode": barcode,
                    "severity": "error"
                })
        
        # Check data types
        if transformed_data.get("weight") is not None:
            try:
                float(transformed_data["weight"])
            except (ValueError, TypeError):
                issues.append({
                    "type": "invalid_data_type",
                    "field": "weight",
                    "barcode": barcode,
                    "severity": "error"
                })
        
        # Check CO2 data integrity
        co2_total = transformed_data.get("co2_total")
        if co2_total is not None:
            try:
                if not (0 <= float(co2_total) <= 10000):
                    issues.append({
                        "type": "invalid_co2_range",
                        "field": "co2_total",
                        "value": co2_total,
                        "barcode": barcode,
                        "severity": "warning"
                    })
            except (ValueError, TypeError):
                issues.append({
                    "type": "invalid_data_type",
                    "field": "co2_total",
                    "barcode": barcode,
                    "severity": "error"
                })
        
        return {
            "database_ready": len([issue for issue in issues if issue["severity"] == "error"]) == 0,
            "issues": issues,
            "warnings_count": len([issue for issue in issues if issue["severity"] == "warning"]),
            "errors_count": len([issue for issue in issues if issue["severity"] == "error"])
        }
    
    def get_quality_stats(self) -> Dict[str, Any]:
        """Get quality checking statistics"""
        return self.quality_stats.copy()
    
    def get_quality_issues(self) -> List[Dict[str, Any]]:
        """Get detected quality issues"""
        return self.quality_issues.copy()
    
    def reset_stats(self):
        """Reset quality checking statistics"""
        self.quality_stats = {
            "products_checked": 0,
            "database_ready_products": 0,
            "quality_issues_found": 0,
            "integrity_checks_performed": 0
        }
        self.quality_issues = []