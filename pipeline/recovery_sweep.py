"""Tier-1 abstract recovery sweep (GitHub Actions entry).

Selects freshly-ingested papers that have no abstract and that the sweep has
not touched yet, recovers them via the free layers (S2 → CrossRef → Unpaywall),
and marks the residual's abstract_recovery_state so it is never re-processed.

Runs as the final step of ingest.yml (immediately after new papers land) and/or
on its own schedule. Newest-first so just-ingested papers are filled ASAP.

Env (GHA secrets): DATABASE_URL, S2_API_KEY[, S2_API_KEY_2...], OPENALEX_EMAIL,
CROSSREF_EMAIL, UNPAYWALL_EMAIL.

Usage:
    python recovery_sweep.py [--limit 3000] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import get_client, execute  # noqa: E402
import abstract_recovery as ar  # noqa: E402

SELECT_FRESH = """
SELECT id::text AS id, doi, title
FROM papers
WHERE abstract IS NULL AND doi IS NOT NULL
  AND (abstract_recovery_state IS NULL
       OR (abstract_recovery_state = 'pending'
           AND abstract_recovery_last < NOW() - INTERVAL '2 days'))
ORDER BY created_at DESC NULLS LAST
LIMIT %s
"""

STATE_DIST_SQL = """
SELECT COALESCE(abstract_recovery_state, '(null)') AS state, COUNT(*) c
FROM papers WHERE abstract IS NULL AND doi IS NOT NULL
GROUP BY 1 ORDER BY c DESC
"""


def collect_s2_keys() -> list[str]:
    keys = []
    if os.environ.get("S2_API_KEY"):
        keys.append(os.environ["S2_API_KEY"])
    for i in range(2, 10):
        k = os.environ.get(f"S2_API_KEY_{i}")
        if k:
            keys.append(k)
    return keys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=3000,
                    help="Max fresh papers to process this run")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = get_client()
    papers = execute(conn, SELECT_FRESH, [args.limit])
    print(f"[recovery] {len(papers)} fresh papers without abstract (state IS NULL)")
    if not papers:
        conn.close()
        return

    keys = collect_s2_keys()
    email = (os.environ.get("UNPAYWALL_EMAIL")
             or os.environ.get("CROSSREF_EMAIL")
             or os.environ.get("OPENALEX_EMAIL"))
    print(f"[recovery] S2 keys={len(keys)}  email={'set' if email else 'MISSING'}  "
          f"dry_run={args.dry_run}")

    stats = ar.recover_batch(conn, papers, s2_keys=keys, email=email,
                             dry_run=args.dry_run)

    print("[recovery] summary:")
    for k in sorted(stats):
        print(f"   {k:18s} {stats[k]}")
    recovered = sum(v for k, v in stats.items() if k.startswith("recovered_"))
    print(f"[recovery] TOTAL recovered: {recovered}/{stats.get('processed', 0)}")

    print("[recovery] residual state distribution (abstract still NULL):")
    for r in execute(conn, STATE_DIST_SQL):
        print(f"   {r['state']:16s} {r['c']}")
    conn.close()


if __name__ == "__main__":
    main()
