#!/usr/bin/env python3
"""
Deduplicate papers in the database.

This script identifies and removes duplicate papers using:
1. DOI matching (most reliable)
2. OpenAlex ID matching
3. Fuzzy title + author + year matching

Usage:
    python deduplicate.py [--dry-run] [--method all|doi|title]
"""

import argparse
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from db import get_client, execute, execute_write


def normalize_doi(doi: str | None) -> str | None:
    """Normalize DOI for comparison."""
    if not doi:
        return None
    doi = doi.lower().strip()
    doi = re.sub(r'^https?://doi\.org/', '', doi)
    doi = re.sub(r'^doi:', '', doi)
    return doi


def normalize_title(title: str | None) -> str:
    """Normalize title for fuzzy comparison."""
    if not title:
        return ""
    # Lowercase
    title = title.lower()
    # Remove HTML tags
    title = re.sub(r'<[^>]+>', '', title)
    # Remove punctuation
    title = re.sub(r'[^\w\s]', '', title)
    # Normalize whitespace
    title = ' '.join(title.split())
    # Take first 100 chars
    return title[:100]


def get_first_author_lastname(authors: list | None) -> str:
    """Extract first author's last name."""
    if not authors or not isinstance(authors, list) or len(authors) == 0:
        return ""

    first_author = authors[0]
    if isinstance(first_author, dict):
        name = first_author.get("name", "")
    else:
        name = str(first_author)

    # Get last word as last name
    parts = name.strip().split()
    if parts:
        return parts[-1].lower()
    return ""


def create_signature(paper: dict) -> str:
    """Create a signature for fuzzy matching."""
    title = normalize_title(paper.get("title"))[:50]
    first_author = get_first_author_lastname(paper.get("authors"))
    published_date = paper.get("published_date") or ""
    year = published_date[:4] if published_date else ""
    return f"{title}|{first_author}|{year}"


def find_duplicates_by_doi(papers: list) -> list[tuple]:
    """Find duplicates by DOI."""
    doi_to_papers = defaultdict(list)

    for paper in papers:
        doi = normalize_doi(paper.get("doi"))
        if doi:
            doi_to_papers[doi].append(paper)

    duplicates = []
    for doi, paper_list in doi_to_papers.items():
        if len(paper_list) > 1:
            # Keep the one with more data (upvotes, comments, etc.)
            sorted_papers = sorted(
                paper_list,
                key=lambda p: (
                    p.get("upvote_count") or 0,
                    p.get("comment_count") or 0,
                    len(p.get("abstract") or ""),
                    p.get("created_at") or ""
                ),
                reverse=True
            )
            keep = sorted_papers[0]
            remove = sorted_papers[1:]
            for paper in remove:
                duplicates.append((keep["id"], paper["id"], "doi", doi))

    return duplicates


def find_duplicates_by_openalex_id(papers: list) -> list[tuple]:
    """Find duplicates by OpenAlex ID."""
    id_to_papers = defaultdict(list)

    for paper in papers:
        openalex_id = paper.get("openalex_id")
        if openalex_id:
            id_to_papers[openalex_id].append(paper)

    duplicates = []
    for oa_id, paper_list in id_to_papers.items():
        if len(paper_list) > 1:
            sorted_papers = sorted(
                paper_list,
                key=lambda p: (
                    p.get("upvote_count") or 0,
                    p.get("comment_count") or 0,
                    len(p.get("abstract") or ""),
                    p.get("created_at") or ""
                ),
                reverse=True
            )
            keep = sorted_papers[0]
            remove = sorted_papers[1:]
            for paper in remove:
                duplicates.append((keep["id"], paper["id"], "openalex_id", oa_id))

    return duplicates


def find_duplicates_by_signature(papers: list) -> list[tuple]:
    """Find duplicates by title + author + year signature."""
    sig_to_papers = defaultdict(list)

    for paper in papers:
        sig = create_signature(paper)
        if sig and sig != "||":  # Skip empty signatures
            sig_to_papers[sig].append(paper)

    duplicates = []
    for sig, paper_list in sig_to_papers.items():
        if len(paper_list) > 1:
            # Skip if papers have different DOIs (not actually duplicates)
            dois = {normalize_doi(p.get("doi")) for p in paper_list if p.get("doi")}
            if len(dois) > 1:
                continue

            sorted_papers = sorted(
                paper_list,
                key=lambda p: (
                    1 if p.get("doi") else 0,  # Prefer papers with DOI
                    p.get("upvote_count") or 0,
                    p.get("comment_count") or 0,
                    len(p.get("abstract") or ""),
                    p.get("created_at") or ""
                ),
                reverse=True
            )
            keep = sorted_papers[0]
            remove = sorted_papers[1:]
            for paper in remove:
                duplicates.append((keep["id"], paper["id"], "signature", sig))

    return duplicates


def main():
    parser = argparse.ArgumentParser(description="Deduplicate papers in database")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually delete")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation (for automated runs)")
    parser.add_argument("--method", choices=["all", "doi", "openalex", "signature"], default="all",
                       help="Deduplication method")
    parser.add_argument("--limit", type=int, default=10000, help="Max papers to process")
    parser.add_argument("--days", type=int, default=None,
                       help="Only consider papers created in the last N days (default: all)")
    args = parser.parse_args()

    print("=" * 60)
    print("Paper Deduplication")
    print("=" * 60)

    conn = get_client()

    try:
        # Fetch candidate papers
        cutoff = None
        if args.days is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
            print(f"Fetching papers created after {cutoff} (limit: {args.limit})...")
        else:
            print(f"Fetching papers (limit: {args.limit})...")

        if cutoff:
            papers = execute(conn,
                """SELECT id, title, authors, doi, openalex_id, published_date,
                          abstract, upvote_count, comment_count, created_at
                   FROM papers
                   WHERE created_at >= %s
                   ORDER BY created_at DESC
                   LIMIT %s""",
                [cutoff, args.limit])
        else:
            papers = execute(conn,
                """SELECT id, title, authors, doi, openalex_id, published_date,
                          abstract, upvote_count, comment_count, created_at
                   FROM papers
                   ORDER BY created_at DESC
                   LIMIT %s""",
                [args.limit])

        print(f"Loaded {len(papers)} papers")

        # Find duplicates
        all_duplicates = []

        if args.method in ["all", "doi"]:
            print("\nFinding duplicates by DOI...")
            doi_dups = find_duplicates_by_doi(papers)
            print(f"  Found {len(doi_dups)} duplicates")
            all_duplicates.extend(doi_dups)

        if args.method in ["all", "openalex"]:
            print("\nFinding duplicates by OpenAlex ID...")
            oa_dups = find_duplicates_by_openalex_id(papers)
            print(f"  Found {len(oa_dups)} duplicates")
            all_duplicates.extend(oa_dups)

        if args.method in ["all", "signature"]:
            print("\nFinding duplicates by signature (title + author + year)...")
            sig_dups = find_duplicates_by_signature(papers)
            print(f"  Found {len(sig_dups)} duplicates")
            all_duplicates.extend(sig_dups)

        # Deduplicate the duplicate list (same paper might be flagged multiple times)
        seen_removals = set()
        unique_duplicates = []
        for keep_id, remove_id, method, value in all_duplicates:
            if remove_id not in seen_removals:
                unique_duplicates.append((keep_id, remove_id, method, value))
                seen_removals.add(remove_id)

        print(f"\nTotal unique duplicates to remove: {len(unique_duplicates)}")

        if not unique_duplicates:
            print("No duplicates found!")
            return

        # Show sample
        print("\nSample duplicates:")
        for keep_id, remove_id, method, value in unique_duplicates[:10]:
            print(f"  Keep: {keep_id[:8]}... Remove: {remove_id[:8]}... ({method}: {value[:40]}...)")

        if len(unique_duplicates) > 10:
            print(f"  ... and {len(unique_duplicates) - 10} more")

        if args.dry_run:
            print("\n[DRY RUN] Would remove duplicates but --dry-run specified")
            return

        # Confirm deletion
        print(f"\nAbout to delete {len(unique_duplicates)} duplicate papers.")
        if not args.yes:
            confirm = input("Continue? (y/n): ")
            if confirm.lower() != 'y':
                print("Aborted.")
                return

        # Delete duplicates
        print("\nDeleting duplicates...")
        deleted = 0
        errors = 0

        for keep_id, remove_id, method, value in unique_duplicates:
            try:
                # Delete related records first (paper_disciplines, paper_tags, etc.)
                execute_write(conn, "DELETE FROM paper_disciplines WHERE paper_id = %s", [remove_id])
                execute_write(conn, "DELETE FROM paper_tags WHERE paper_id = %s", [remove_id])

                # Delete the paper
                execute_write(conn, "DELETE FROM papers WHERE id = %s", [remove_id])
                deleted += 1

                if deleted % 100 == 0:
                    print(f"  Deleted {deleted} papers...")

            except Exception as e:
                print(f"  Error deleting {remove_id}: {e}")
                conn.rollback()
                errors += 1

        print(f"\nDone! Deleted {deleted} duplicates, {errors} errors")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
