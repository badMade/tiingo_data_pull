"""Tiingo data pull package."""

__version__ = "0.1.0"

from .services.pipeline import TiingoToNotionPipeline

__all__ = ["TiingoToNotionPipeline", "__version__"]
