"""
Database client for LitPulse pipeline.
"""
import os
import time
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Simple retry for Supabase operations
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def _is_retryable(e: Exception) -> bool:
    """Check if an error is transient and worth retrying."""
    err_str = str(e)
    # Postgres error codes that should NOT be retried
    # 23505 = unique_violation (duplicate key)
    # 23503 = foreign_key_violation
    # 23502 = not_null_violation
    if "'23505'" in err_str or "'23503'" in err_str or "'23502'" in err_str:
        return False
    return True


def _retry(fn, *args, **kwargs):
    """Retry a function with exponential backoff. Skips non-retryable errors."""
    for attempt in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if not _is_retryable(e) or attempt == MAX_RETRIES - 1:
                raise
            delay = RETRY_DELAY * (2 ** attempt)
            print(f"[DB] Retry {attempt + 1}/{MAX_RETRIES} after {delay}s: {e}")
            time.sleep(delay)


def get_client() -> Client:
    """Get Supabase client."""
    url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    return create_client(url, key)


def upsert_paper(client: Client, paper: dict) -> str | None:
    """
    Insert or update a paper. Returns paper ID if successful.
    Deduplication is based on openalex_id, doi, or arxiv_id.
    Retries on transient failures.
    """
    def _do_upsert():
        # Check for existing paper
        existing = None

        if paper.get("openalex_id"):
            result = client.table("papers").select("id").eq("openalex_id", paper["openalex_id"]).execute()
            if result.data:
                existing = result.data[0]

        if not existing and paper.get("doi"):
            result = client.table("papers").select("id").eq("doi", paper["doi"]).execute()
            if result.data:
                existing = result.data[0]

        if not existing and paper.get("arxiv_id"):
            result = client.table("papers").select("id").eq("arxiv_id", paper["arxiv_id"]).execute()
            if result.data:
                existing = result.data[0]

        if existing:
            # Update existing paper
            client.table("papers").update(paper).eq("id", existing["id"]).execute()
            return existing["id"]
        else:
            # Insert new paper
            result = client.table("papers").insert(paper).execute()
            if result.data:
                return result.data[0]["id"]

        return None

    return _retry(_do_upsert)


def get_disciplines_map(client: Client) -> dict[str, str]:
    """Get mapping of discipline slug -> id."""
    result = client.table("disciplines").select("id, slug").execute()
    return {d["slug"]: d["id"] for d in result.data} if result.data else {}


def get_tags_map(client: Client) -> dict[str, str]:
    """Get mapping of tag name -> id."""
    result = client.table("tags").select("id, name").execute()
    return {t["name"]: t["id"] for t in result.data} if result.data else {}


def link_paper_to_discipline(client: Client, paper_id: str, discipline_id: str, source: str = "openalex") -> None:
    """Link a paper to a discipline."""
    try:
        client.table("paper_disciplines").upsert({
            "paper_id": paper_id,
            "discipline_id": discipline_id,
            "source": source,
        }, on_conflict="paper_id,discipline_id").execute()
    except Exception as e:
        print(f"[DB] Error linking paper to discipline: {e}")


def link_paper_to_tag(client: Client, paper_id: str, tag_id: str, source: str = "openalex") -> None:
    """Link a paper to a tag."""
    try:
        client.table("paper_tags").upsert({
            "paper_id": paper_id,
            "tag_id": tag_id,
            "source": source,
        }, on_conflict="paper_id,tag_id").execute()
    except Exception as e:
        print(f"[DB] Error linking paper to tag: {e}")


def get_disciplines(client: Client) -> list[dict]:
    """Get all disciplines."""
    result = client.table("disciplines").select("*").order("display_order").execute()
    return result.data if result.data else []


def get_recent_papers_by_discipline(client: Client, discipline_id: str, days: int = 7) -> list[dict]:
    """Get recent papers for a specific discipline."""
    from datetime import datetime, timedelta

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    result = client.table("paper_disciplines").select(
        "paper_id, papers(*)"
    ).eq("discipline_id", discipline_id).gte("papers.created_at", cutoff).execute()

    return [r["papers"] for r in result.data if r.get("papers")] if result.data else []


def get_papers_without_summary(client: Client, limit: int = 100) -> list[dict]:
    """Get papers that don't have a summary yet."""
    result = client.table("papers").select(
        "id, title, abstract"
    ).is_("id", "not.in",
        client.table("summaries").select("paper_id")
    ).not_.is_("abstract", None).limit(limit).execute()

    # Note: The above query might not work as expected.
    # Alternative approach:
    result = client.rpc("get_papers_without_summary", {"limit_count": limit}).execute()

    return result.data if result.data else []


def save_summary(client: Client, paper_id: str, summary: dict, model: str) -> None:
    """Save an AI-generated summary."""
    data = {
        "paper_id": paper_id,
        "llm_model": model,
        "so_what": summary.get("so_what"),
        "contribution": summary.get("contribution"),
        "methodology": summary.get("methodology"),
        "data_info": summary.get("data"),
        "key_finding": summary.get("key_finding"),
        "limitations": summary.get("limitations"),
        "tags": summary.get("tags", []),
    }

    client.table("summaries").upsert(data, on_conflict="paper_id,llm_model").execute()


def save_rankings(client: Client, date: str, discipline_id: str | None, rankings: list[dict]) -> None:
    """Save daily rankings for a discipline."""
    data = [
        {
            "paper_id": r["paper_id"],
            "ranking_date": date,
            "discipline_id": discipline_id,
            "rank_position": r["rank"],
            "score": r["score"],
            "score_breakdown": r.get("breakdown"),
        }
        for r in rankings
    ]

    client.table("daily_rankings").upsert(
        data,
        on_conflict="paper_id,ranking_date,discipline_id"
    ).execute()


def update_paper_latest_rank(client: Client, paper_id: str, rank: int, score: float) -> None:
    """Update denormalized rank on papers table."""
    client.table("papers").update({
        "latest_rank": rank,
        "latest_score": score,
    }).eq("id", paper_id).execute()
