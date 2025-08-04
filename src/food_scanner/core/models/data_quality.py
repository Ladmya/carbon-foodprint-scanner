"""
src/food_scanner/core/models/data_quality.py
Data models for data quality analysis and validation
Main analyze engine
Base analyzer
Barcode analyzer
Text field analyzer
Quantity analyzer
Nutriscore analyzer
CO2 analyzer

"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime
from enum import Enum


class FieldType(Enum):
    """Types de champs analysés"""
    IDENTIFIER = "identifier"        # barcode
    TEXT = "text"                   # product_name, brand_name
    LIST = "list"                   # brand_tags
    NUMERIC = "numeric"             # weight, nutriscore_score, co2_total
    CATEGORICAL = "categorical"     # nutriscore_grade, eco_score
    CALCULATED = "calculated"       # co2_*_km, impact_level
    METADATA = "metadata"           # created_at, updated_at, etc.


class ValidationRule(Enum):
    """Règles de validation disponibles"""
    REQUIRED = "required"           # Champ obligatoire
    OPTIONAL = "optional"           # Champ facultatif
    REJECT_IF_MISSING = "reject_if_missing"  # Rejeter produit si manquant
    FALLBACK_CHAIN = "fallback_chain"        # Chaîne de fallback
    FORMAT_VALIDATION = "format_validation"  # Validation de format
    RANGE_VALIDATION = "range_validation"    # Validation de plage
    ENUMERATION = "enumeration"              # Valeurs autorisées


@dataclass
class FieldValidationRule:
    """Règle de validation pour un champ spécifique"""
    field_name: str
    field_type: FieldType
    validation_type: ValidationRule
    required: bool = False
    fallback_sources: List[str] = field(default_factory=list)
    valid_range: Optional[Tuple[Union[int, float], Union[int, float]]] = None
    valid_values: Optional[List[str]] = None
    format_pattern: Optional[str] = None
    transformation_logic: Optional[str] = None
    business_description: str = ""


@dataclass
class FieldAnalysisResult:
    """Résultat d'analyse pour un champ"""
    field_name: str
    field_type: FieldType
    total_products: int
    
    # Statistiques de présence
    present_count: int = 0
    missing_count: int = 0
    empty_count: int = 0
    invalid_count: int = 0
    
    # Statistiques de qualité
    valid_count: int = 0
    fallback_used_count: int = 0
    transformation_success_count: int = 0
    
    # Métriques calculées
    presence_rate: float = 0.0
    validity_rate: float = 0.0
    quality_score: float = 0.0
    
    # Détails spécifiques au type de champ
    value_distribution: Dict[str, int] = field(default_factory=dict)
    pattern_analysis: Dict[str, Any] = field(default_factory=dict)
    examples: Dict[str, List[Dict]] = field(default_factory=dict)
    
    # Recommandations
    transformation_recommendations: List[str] = field(default_factory=list)
    quality_improvement_suggestions: List[str] = field(default_factory=list)


@dataclass
class ComprehensiveAnalysisReport:
    """Rapport d'analyse complète de tous les champs"""
    analysis_timestamp: str
    dataset_info: Dict[str, Any]
    
    # Résultats par champ
    field_results: Dict[str, FieldAnalysisResult] = field(default_factory=dict)
    
    # Statistiques globales
    total_products_analyzed: int = 0
    overall_quality_score: float = 0.0
    
    # Analyse de rejet
    rejection_analysis: Dict[str, Any] = field(default_factory=dict)
    
    # Règles générées
    generated_transformation_rules: Dict[str, Any] = field(default_factory=dict)
    
    # Recommandations
    critical_issues: List[str] = field(default_factory=list)
    improvement_priorities: List[str] = field(default_factory=list)


@dataclass
class RawProductData:
    """Raw data as extracted from OpenFoodFacts API"""
    barcode: str  # IMPORTANT: String pour préserver zéros de début
    extraction_timestamp: str
    source_api: str
    raw_response: Dict[str, Any]
    api_status: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'barcode': self.barcode,
            'extraction_timestamp': self.extraction_timestamp,
            'source_api': self.source_api,
            'raw_response': self.raw_response,
            'api_status': self.api_status
        }


@dataclass
class TransformedProductData:
    """Clean, validated, business-ready data for single table (fast loading)"""
    barcode: str  # String primary key - préserve zéros de début
    product_name: str
    brand_name: str
    brand_tags: Optional[List[str]]
    weight: Optional[float]
    product_quantity_unit: Optional[str]
    nutriscore_grade: Optional[str]
    nutriscore_score: Optional[float]
    eco_score: Optional[str]
    co2_total: Optional[float]
    
    # Calculated fields pour bot (fast loading)
    co2_vehicle_km: Optional[float] = None
    co2_train_km: Optional[float] = None
    co2_bus_km: Optional[float] = None
    co2_plane_km: Optional[float] = None
    impact_level: Optional[str] = None
    
    # Metadata
    created_at: str = None
    updated_at: str = None
    cache_expires_at: str = None
    transformation_timestamp: str = None
    data_quality_score: float = None
    validation_errors: List[str] = None
    raw_data: Optional[Dict[str, Any]] = None  # JSONB backup
    
    def __post_init__(self):
        if self.transformation_timestamp is None:
            self.transformation_timestamp = datetime.now().isoformat()
        if self.validation_errors is None:
            self.validation_errors = []
        if self.created_at is None:
            now = datetime.now()
            self.created_at = now.isoformat()
            self.updated_at = now.isoformat()
            self.cache_expires_at = now.replace(hour=23, minute=59, second=59).isoformat()


# Utility functions for timestamping
def generate_timestamp_suffix() -> str:
    """Générer suffixe timestamp pour fichiers JSON"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def generate_analysis_filename(prefix: str, extension: str = "json") -> str:
    """Générer nom de fichier avec timestamp pour analyses"""
    timestamp = generate_timestamp_suffix()
    return f"{prefix}_{timestamp}.{extension}"