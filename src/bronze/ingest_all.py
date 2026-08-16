"""Orchestrate Bronze ingestion for all entities and ingestion log."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bronze.bronze_common import (
    BronzeConfig,
    BronzeIngestionError,
    generate_batch_id,
    get_spark_session,
    ingest_all_entities,
)


def main() -> int:
    spark = get_spark_session("bronze-ingest-all")
    config = BronzeConfig.from_env()
    batch_id = generate_batch_id()

    try:
        results = ingest_all_entities(
            spark, config, batch_id=batch_id, write_delta=True
        )
    except BronzeIngestionError as exc:
        print(f"Bronze ingestion failed: {exc}", file=sys.stderr)
        return 1

    print(f"Bronze ingestion complete. batch_id={batch_id}")
    for result in results:
        print(
            f"  {result.entity_name}: {result.row_count} rows "
            f"from {result.source_file} → {result.table_name}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
