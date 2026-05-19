import json
import re
from datetime import datetime, timezone

from bec.strategy_builder import schema
from bec.strategy_builder.templates import PACKAGE_VERSION


PACKAGE_EXTENSION = ".bec-strategy.json"


def slugify_strategy_id(value: str) -> str:
    value = str(value or "").strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "custom_strategy"


def build_export_package(strategy_row) -> dict:
    metadata = schema.parse_json_object(strategy_row.get("Metadata_JSON", "{}"), "Metadata_JSON")
    definition = schema.validate_definition(strategy_row.get("Definition_JSON", "{}"))
    now = datetime.now(timezone.utc).isoformat()
    return {
        "package_version": PACKAGE_VERSION,
        "bec_min_version": "2026-05-12",
        "exported_at": now,
        "strategy": {
            "id": strategy_row.get("Id"),
            "name": strategy_row.get("Name"),
            "description": definition.get("description", ""),
            "author": metadata.get("author", ""),
            "source_url": metadata.get("source_url", ""),
            "license": metadata.get("license", ""),
            "tags": metadata.get("tags", []),
            "type": strategy_row.get("Type", "custom"),
            "parent_strategy_id": strategy_row.get("Parent_Strategy_Id", ""),
            "version": int(strategy_row.get("Version", 1) or 1),
        },
        "definition": definition,
        "compatibility": definition.get("constraints", {}),
    }


def dumps_package(package: dict) -> str:
    return json.dumps(package, ensure_ascii=True, sort_keys=True, indent=2)


def validate_import_package(value) -> dict:
    package = schema.parse_json_object(value, "strategy package")
    if int(package.get("package_version", 0) or 0) != PACKAGE_VERSION:
        raise schema.StrategyValidationError(f"Unsupported package_version. Expected {PACKAGE_VERSION}.")
    strategy_meta = package.get("strategy")
    if not isinstance(strategy_meta, dict):
        raise schema.StrategyValidationError("strategy must be an object.")
    definition = schema.validate_definition(package.get("definition"))
    return {
        "package": package,
        "strategy": strategy_meta,
        "definition": definition,
    }
