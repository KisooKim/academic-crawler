"""
Journal-based paper ingestion for LiterView.

Fetches papers from ALL journals in journals_config.py, batched by discipline.
Each discipline's journals are queried together using OpenAlex OR filter.
"""
import os
import time
import httpx
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from db import get_client, upsert_paper, link_paper_to_discipline, get_disciplines_map
from journals_config import JOURNALS_BY_DISCIPLINE

load_dotenv()

OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "")


def reconstruct_abstract(inverted_index: dict | None) -> str | None:
    """Reconstruct abstract from OpenAlex inverted index format."""
    if not inverted_index:
        return None

    words = []
    for word, positions in inverted_index.items():
        for pos in positions:
            words.append((pos, word))

    words.sort(key=lambda x: x[0])
    return " ".join(word for _, word in words)


def is_valid_paper(title: str, source: str | None, authors: list) -> bool:
    """Validate that this is a real paper."""
    if not title:
        return False

    title_lower = title.strip().lower()
    source_lower = (source or "").strip().lower()

    if source_lower and title_lower == source_lower:
        return False

    if len(title) < 20:
        return False

    if not authors or len(authors) == 0:
        return False

    if all(not a.get("name", "").strip() for a in authors):
        return False

    return True


def normalize_openalex(work: dict) -> dict | None:
    """Convert OpenAlex work to our paper format."""
    title = work.get("title")
    if not title:
        return None

    abstract = work.get("abstract") or reconstruct_abstract(work.get("abstract_inverted_index"))

    authors = []
    for authorship in work.get("authorships", [])[:10]:
        author = authorship.get("author", {})
        institution = ""
        if authorship.get("institutions"):
            institution = authorship["institutions"][0].get("display_name", "")

        authors.append({
            "name": author.get("display_name", ""),
            "affiliation": institution,
            "orcid": author.get("orcid"),
            "openalex_id": author.get("id"),  # e.g. "https://openalex.org/A5023888391"
        })

    url = work.get("doi") or work.get("id")
    pdf_url = None
    if work.get("open_access", {}).get("oa_url"):
        pdf_url = work["open_access"]["oa_url"]

    source = None
    source_openalex_id = None
    primary_location = work.get("primary_location") or {}
    if primary_location.get("source"):
        source = primary_location["source"].get("display_name")
        source_openalex_id = primary_location["source"].get("id")

    if not is_valid_paper(title, source, authors):
        return None

    return {
        "title": title,
        "abstract": abstract,
        "authors": authors,
        "source": source,
        "source_openalex_id": source_openalex_id,
        "published_date": work.get("publication_date"),
        "published_year": work.get("publication_year"),
        "doi": work.get("doi"),
        "url": url,
        "pdf_url": pdf_url,
        "openalex_id": work.get("id"),
        "citation_count": work.get("cited_by_count", 0),
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_papers_by_sources(source_ids: list[str], days: int = 7, per_page: int = 200, max_papers: int = 5000) -> list[dict]:
    """
    Fetch recent papers from multiple sources via OpenAlex.
    Uses OR filter to batch all source IDs for a discipline into one query.
    """
    papers = []
    from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # OpenAlex supports OR with pipe separator
    source_filter = "|".join(source_ids)

    url = "https://api.openalex.org/works"
    params = {
        "filter": f"from_publication_date:{from_date},primary_location.source.id:{source_filter}",
        "sort": "publication_date:desc",
        "per-page": per_page,
    }
    if OPENALEX_EMAIL:
        params["mailto"] = OPENALEX_EMAIL

    page = 1
    while len(papers) < max_papers:
        params["page"] = page
        response = httpx.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            break

        for work in results:
            paper = normalize_openalex(work)
            if paper:
                papers.append(paper)

        # Check if more pages
        total_count = data.get("meta", {}).get("count", 0)
        if page * per_page >= total_count:
            break

        page += 1
        time.sleep(0.1)

    return papers


def main(days: int = 7, per_page: int = 200):
    """Run the journal-based ingestion pipeline."""
    print(f"[Ingest] Starting journal-based ingestion at {datetime.now().isoformat()}")
    print(f"[Ingest] Fetching papers from last {days} days")

    # Count total journals
    total_journals = sum(len(journals) for journals in JOURNALS_BY_DISCIPLINE.values())
    print(f"[Ingest] {len(JOURNALS_BY_DISCIPLINE)} disciplines, {total_journals} journals")

    client = get_client()
    disciplines_map = get_disciplines_map(client)
    print(f"[Ingest] Loaded {len(disciplines_map)} disciplines from DB")

    # Build reverse map: OpenAlex source URL -> config journal name
    source_id_to_name = {}
    for disc, journals in JOURNALS_BY_DISCIPLINE.items():
        for src_id, name in journals.items():
            source_id_to_name[f"https://openalex.org/{src_id}"] = name

    all_papers = []
    seen_ids = set()

    for disc_idx, (discipline_slug, journals) in enumerate(JOURNALS_BY_DISCIPLINE.items(), 1):
        discipline_id = disciplines_map.get(discipline_slug)
        if not discipline_id:
            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug}: not in DB, skipping")
            continue

        source_ids = list(journals.keys())
        if not source_ids:
            continue

        try:
            papers = fetch_papers_by_sources(source_ids, days=days, per_page=per_page)
            new_count = 0

            for paper in papers:
                openalex_id = paper.get("openalex_id")
                if openalex_id and openalex_id not in seen_ids:
                    seen_ids.add(openalex_id)
                    # Normalize source name to match config
                    src_oa_id = paper.pop("source_openalex_id", None)
                    if src_oa_id and src_oa_id in source_id_to_name:
                        paper["source"] = source_id_to_name[src_oa_id]
                    paper["_discipline_id"] = discipline_id
                    paper["_discipline_slug"] = discipline_slug
                    all_papers.append(paper)
                    new_count += 1

            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug} ({len(source_ids)} journals): {new_count} papers")
            time.sleep(0.1)

        except Exception as e:
            print(f"[{disc_idx}/{len(JOURNALS_BY_DISCIPLINE)}] {discipline_slug}: Error - {e}")

    print(f"\n[Ingest] Collected {len(all_papers)} unique papers from {len(JOURNALS_BY_DISCIPLINE)} disciplines")

    # Save to database
    saved = 0
    linked = 0
    for paper in all_papers:
        try:
            discipline_id = paper.pop("_discipline_id")
            discipline_slug = paper.pop("_discipline_slug")

            paper_id = upsert_paper(client, paper)
            if paper_id:
                saved += 1
                link_paper_to_discipline(client, paper_id, discipline_id, source="journal")
                linked += 1

        except Exception as e:
            print(f"[Ingest] Error saving paper: {e}")

    print(f"[Ingest] Saved {saved} papers to database")
    print(f"[Ingest] Linked {linked} papers to disciplines")
    print(f"[Ingest] Completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Number of days to look back")
    parser.add_argument("--per-page", type=int, default=200, help="Papers per API page")
    args = parser.parse_args()

    main(days=args.days, per_page=args.per_page)
