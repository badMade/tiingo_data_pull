"""Compatibility exports for the Notion integration."""
from __future__ import annotations

from ..integrations.notion_client import (
    NotionClient,
    NotionDatabaseConfig,
    NotionPropertyMapping,
    load_notion_config,
)

NotionPropertyConfig = NotionPropertyMapping

__all__ = [
    "NotionClient",
    "NotionDatabaseConfig",
    "NotionPropertyConfig",
    "NotionPropertyMapping",
    "load_notion_config",
]
