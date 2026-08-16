"""Bronze ingestion for orders.csv → bronze_orders."""

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
    spark = get_spark_session("bronze-ingest-orders")
    config = BronzeConfig.from_env()
    batch_id = generate_batch_id()
    ingestion_timestamp = datetime.now(timezone.utc)

    try:
        result = ingest_entity(
            spark,
            "orders",
            config,
            batch_id,
            ingestion_timestamp,
            write_delta=True,
        )
    except BronzeIngestionError as exc:
        print(f"Bronze orders ingestion failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"Bronze orders ingestion complete: "
        f"{result.row_count} rows, batch_id={result.batch_id}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
