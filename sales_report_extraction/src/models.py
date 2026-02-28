from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class ValidationResult:
    """Standardized Data Contract for parser validation returns."""
    status: str  # Strictly "PASSED", "FAILED", or "UNVALIDATED"
    message: str
    metrics: Dict[str, Any]
