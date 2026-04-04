"""Preview and repair stale pipeline run records."""

from __future__ import annotations

import argparse

from pipelines.common import get_client, repair_stale_runs


def build_repair_summary_sql(max_age_minutes: int = 30, layer: str | None = None) -> str:
    layer_filter = f" AND layer = '{layer}'" if layer else ""
    return f"""
SELECT
    layer,
    table_name,
    bar_interval_sec,
    count() AS stale_run_count,
    min(started_at) AS oldest_started_at
FROM meta.pipeline_runs
WHERE status = 'running'
  AND started_at < now() - INTERVAL {max_age_minutes} MINUTE{layer_filter}
GROUP BY layer, table_name, bar_interval_sec
ORDER BY layer, table_name, bar_interval_sec
"""


def preview_stale_runs(client, max_age_minutes: int = 30, layer: str | None = None):
    return client.query(build_repair_summary_sql(max_age_minutes=max_age_minutes, layer=layer))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-age-minutes", type=int, default=30)
    parser.add_argument("--layer")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    client = get_client()
    result = preview_stale_runs(client, max_age_minutes=args.max_age_minutes, layer=args.layer)
    if result.result_rows:
        for row in result.result_rows:
            print(row)
    else:
        print("No stale running pipeline runs found.")

    if not args.apply:
        return

    repair_stale_runs(client, max_age_minutes=args.max_age_minutes, layer=args.layer)
    print("Marked stale running pipeline runs as failed.")


if __name__ == "__main__":
    main()
