"""Bronze ingestion for products.csv → bronze_products."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bronze.bronze_common import (
    BronzeConfig,
    BronzeIngestionError,
    generate_batch_id,
    get_spark_session,
    ingest_entity,
)


def main() -> int:
    spark = get_spark_session("bronze-ingest-products")
    config = BronzeConfig.from_env()
    batch_id = generate_batch_id()
    ingestion_timestamp = datetime.now(timezone.utc)

    try:
        result = ingest_entity(
            spark,
            "products",
            config,
            batch_id,
            ingestion_timestamp,
            write_delta=True,
        )
    except BronzeIngestionError as exc:
        print(f"Bronze products ingestion failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"Bronze products ingestion complete: "
        f"{result.row_count} rows, batch_id={result.batch_id}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
