"""drain_capture_queue.py — resolve the capture queue after each scheduled crawl (W-E3, D-W3-6).

Charter: charters/2026-09-08-ext-we3-capture-charter.md, decision D-W3-6. A capture that missed
the corpus at click time left one `library_capture_queue` row (migration 047) next to its
EXTERNAL library item (`paper_id IS NULL`, the scraped record in `ext_meta`). This script re-runs
the lookup the capture route ran and either links the item to the corpus row that has since
arrived, or leaves it external for the next crawl. It is the entry point the scheduled crawl
(another repository) calls; there is no scheduler wiring here.

Invariants:
  * NEVER writes to `papers`. No corpus ingest, no metadata fetch from any external source, no
    import of the journal configuration: the ingest half of the drain was deferred out of this
    slice (charter v2 -> v3, owner decision 2026-09-08). An absent paper is simply not in the
    corpus yet, so a miss stays `pending` with `attempts + 1` and no attempt cap.
  * Every version bump goes through the LOCKED COUNTER — `merge_papers.BUMP_SQL`, the statement
    `lib/library/version.ts` uses — taken FIRST in the row's transaction, before any row of that
    user's data is touched (the same lock order as the app and the corpus merge). A bump that
    bypasses the counter can be ordered below a concurrent web push, and every client's sync
    cursor then skips the drain's write entirely.
  * Linking is merge-aware and goes through the fold `merge_papers` already has
    (`_fold_library_rows`). Setting `paper_id` on the external row while the user already holds
    a row for that paper would violate UNIQUE (user_id, paper_id); instead the external row is
    folded into that row and tombstoned with `merged_into` naming it, which the sync client's
    AD-12 redirect map consumes to rewrite pending ops onto the winner.

Status lifecycle (047): pending -> linked (no row for the paper: re-point, ext_meta := NULL)
| merged (the user already held a row: fold + tombstone) | closed (the item was tombstoned
before the drain ran) | failed (an error the drain could not classify; `last_error` holds it).
One transaction per queue row, committed per row: a failure on one row rolls back that row only.

  python pipeline/drain_capture_queue.py [--dry-run] [--limit N] [--dsn URL]

`--dsn` defaults to DATABASE_URL from the environment (.env.local is loaded, never overriding).
`--dry-run` performs the lookups and prints each row's decision without writing.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
load_dotenv(ROOT / ".env.local", override=False)

from merge_papers import BUMP_SQL, _fold_library_rows, has_column, has_table  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")   # Windows cp1252 console
except Exception:
    pass

# The row shape `_fold_library_rows` takes (merge_papers `cols`, verbatim).
FOLD_COLS = "id, deleted_at IS NULL, starred, read_status, memo, added_at, ext_meta"

# Both sides of the arXiv comparison are version-stripped (047: papers.arxiv_id is stored
# version-suffixed, e.g. 2608.10993v1; idx_papers_arxiv_id_base backs the papers side).
ARXIV_VERSION = re.compile(r"v[0-9]+$")

OUTCOMES = ("pending", "linked", "merged", "closed", "failed")


def strip_arxiv_version(arxiv_id: str | None) -> str | None:
    return ARXIV_VERSION.sub("", arxiv_id) if arxiv_id else None


def lookup_paper(cur, doi: str | None, arxiv_id: str | None) -> str | None:
    """The capture route's lookup: papers by DOI, papers by version-stripped arXiv id, then the
    redirect table by the same two keys (newest redirect wins). An arm whose key is NULL is
    skipped. Returns the `papers.id` hit, or None."""
    base = strip_arxiv_version(arxiv_id)
    if doi:
        cur.execute("SELECT id FROM papers WHERE doi IS NOT NULL "
                    "AND normalize_doi(doi) = normalize_doi(%s)", [doi])
        row = cur.fetchone()
        if row:
            return str(row[0])
    if base:
        cur.execute("SELECT id FROM papers WHERE arxiv_id IS NOT NULL "
                    "AND regexp_replace(arxiv_id, 'v[0-9]+$', '') = %s", [base])
        row = cur.fetchone()
        if row:
            return str(row[0])
    if doi:
        cur.execute("SELECT new_id FROM paper_redirects WHERE old_doi IS NOT NULL "
                    "AND normalize_doi(old_doi) = normalize_doi(%s) "
                    "ORDER BY merged_at DESC LIMIT 1", [doi])
        row = cur.fetchone()
        if row:
            return str(row[0])
    if base:
        cur.execute("SELECT new_id FROM paper_redirects WHERE old_arxiv_id IS NOT NULL "
                    "AND regexp_replace(old_arxiv_id, 'v[0-9]+$', '') = %s "
                    "ORDER BY merged_at DESC LIMIT 1", [base])
        row = cur.fetchone()
        if row:
            return str(row[0])
    return None


def _resolve(cur, qid: str, status: str) -> None:
    cur.execute("UPDATE library_capture_queue SET status = %s, resolved_at = now() WHERE id = %s",
                [status, qid])


def process_row(conn, qid: str, *, collections: bool, merged_into: bool,
                dry_run: bool) -> str | None:
    """One queue row in one transaction. Returns the outcome (one of OUTCOMES), or None when the
    row was skipped (no longer pending, or its item is gone). Raises on any error; the caller
    owns the rollback and the `failed` stamp."""
    cur = conn.cursor()
    lock = "" if dry_run else " FOR UPDATE"
    cur.execute("SELECT id, library_item_id, user_id, doi, arxiv_id, status "
                f"FROM library_capture_queue WHERE id = %s{lock}", [qid])
    row = cur.fetchone()
    if row is None or row[5] != "pending":
        print(f"  [skip]    queue={qid} status={row[5] if row else 'gone'}")
        return None
    _, item_id, user, doi, arxiv_id, _ = row
    item_id, user = str(item_id), str(user)

    cur.execute("SELECT id, deleted_at, paper_id FROM library_items WHERE id = %s", [item_id])
    item = cur.fetchone()
    if item is None:
        # Cascade-deleted: the queue row is gone with it (ON DELETE CASCADE); nothing to do.
        print(f"  [skip]    queue={qid} item={item_id} gone")
        return None
    if item[1] is not None:
        print(f"  [closed]  queue={qid} item={item_id} (tombstoned before the drain ran)")
        if not dry_run:
            _resolve(cur, qid, "closed")
        return "closed"
    if item[2] is not None:
        print(f"  [linked]  queue={qid} item={item_id} paper={item[2]} (already linked)")
        if not dry_run:
            _resolve(cur, qid, "linked")
        return "linked"

    paper = lookup_paper(cur, doi, arxiv_id)
    if paper is None:
        print(f"  [pending] queue={qid} item={item_id} doi={doi!r} arxiv={arxiv_id!r} (miss)")
        if not dry_run:
            cur.execute("UPDATE library_capture_queue SET attempts = attempts + 1 WHERE id = %s",
                        [qid])
        return "pending"

    if dry_run:
        cur.execute("SELECT id FROM library_items WHERE user_id = %s AND paper_id = %s",
                    [user, paper])
        w = cur.fetchone()
        if w is None:
            print(f"  [linked]  queue={qid} item={item_id} paper={paper}")
            return "linked"
        print(f"  [merged]  queue={qid} item={item_id} paper={paper} into={w[0]}")
        return "merged"

    # Hit. BUMP FIRST (lock order; merge_papers.BUMP_SQL), then read the two rows the fold takes.
    cur.execute(BUMP_SQL, [user])
    v = cur.fetchone()[0]
    cur.execute(f"SELECT {FOLD_COLS} FROM library_items WHERE user_id = %s AND paper_id = %s",
                [user, paper])
    w_row = cur.fetchone()   # UNIQUE (user_id, paper_id) => at most one
    cur.execute(f"SELECT {FOLD_COLS} FROM library_items WHERE id = %s", [item_id])
    l_row = cur.fetchone()

    stats = _fold_library_rows(cur, user, l_row, w_row, paper, v,
                               collections=collections, merged_into=merged_into)
    if w_row is None:
        print(f"  [linked]  queue={qid} item={item_id} paper={paper} v={v}")
        _resolve(cur, qid, "linked")
        return "linked"
    touched = {k: n for k, n in stats.items() if n}
    print(f"  [merged]  queue={qid} item={item_id} paper={paper} into={w_row[0]} v={v} {touched}")
    _resolve(cur, qid, "merged")
    return "merged"


def run(conn, *, dry_run: bool = False, limit: int | None = None) -> dict[str, int]:
    cur = conn.cursor()
    # Capability probes, once per run. 045 is applied in production, so a missing `merged_into`
    # is a mis-provisioned environment, not a supported state: abort before touching a row.
    collections = has_table(cur, "public.library_collection_items")
    merged_into = has_column(cur, "library_items", "merged_into")
    conn.rollback()
    if not merged_into:
        raise SystemExit("[abort] library_items.merged_into is absent — migration 045 is applied "
                         "in production, so this environment is mis-provisioned; refusing to run")

    sql = "SELECT id FROM library_capture_queue WHERE status = 'pending' ORDER BY created_at"
    params: list = []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cur.execute(sql, params)
    ids = [str(r[0]) for r in cur.fetchall()]
    conn.rollback()
    print(f"[drain] {len(ids)} pending queue row(s){' [dry-run]' if dry_run else ''}")

    counts = {k: 0 for k in OUTCOMES}
    for qid in ids:
        try:
            outcome = process_row(conn, qid, collections=collections, merged_into=merged_into,
                                  dry_run=dry_run)
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception as e:  # noqa: BLE001 — one row's failure must not stop the run
            conn.rollback()
            err = f"{e.__class__.__name__}: {e}"
            print(f"  [failed]  queue={qid} {err}")
            if not dry_run:
                c2 = conn.cursor()
                c2.execute("UPDATE library_capture_queue SET status = 'failed', last_error = %s, "
                           "attempts = attempts + 1 WHERE id = %s", [err, qid])
                conn.commit()
            outcome = "failed"
        if outcome is not None:
            counts[outcome] += 1

    if dry_run:
        print("[dry-run] no writes performed")
    print(" ".join(f"{k}={counts[k]}" for k in OUTCOMES))
    return counts


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="resolve pending library_capture_queue rows (D-W3-6)")
    ap.add_argument("--dry-run", action="store_true", help="look up and print decisions; write nothing")
    ap.add_argument("--limit", type=int, help="process at most N pending rows (oldest first)")
    ap.add_argument("--dsn", help="database URL (default: DATABASE_URL from the environment)")
    a = ap.parse_args(argv)

    dsn = a.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        print("[abort] no --dsn given and DATABASE_URL is not set")
        return 1
    try:
        conn = psycopg2.connect(dsn, connect_timeout=20)
    except psycopg2.Error as e:
        print(f"[abort] could not connect: {e.__class__.__name__}: {e}")
        return 1
    conn.autocommit = False
    try:
        run(conn, dry_run=a.dry_run, limit=a.limit)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
