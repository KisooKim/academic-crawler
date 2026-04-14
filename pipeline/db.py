"""
Database client for LiterView pipeline.
Uses psycopg2 to connect directly to Neon PostgreSQL.
"""
import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Get a PostgreSQL connection."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("Missing DATABASE_URL")
    return psycopg2.connect(url)


def get_client():
    """Get a database connection (backward-compatible name)."""
    return get_connection()


def execute(conn, sql, params=None):
    """Execute a query and return rows as list of dicts."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        if cur.description:
            return [dict(row) for row in cur.fetchall()]
        return []


def execute_one(conn, sql, params=None):
    """Execute a query and return a single row dict, or None."""
    rows = execute(conn, sql, params)
    return rows[0] if rows else None


def execute_write(conn, sql, params=None):
    """Execute a write query (INSERT/UPDATE/DELETE) and commit."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        conn.commit()
        if cur.description:
            return [dict(row) for row in cur.fetchall()]
        return []


def upsert_paper(conn, paper: dict) -> str | None:
    """
    Insert or update a paper. Returns paper ID if successful.
    Deduplication is based on openalex_id, doi, or arxiv_id.
    """
    existing = None

    if paper.get("openalex_id"):
        existing = execute_one(conn, "SELECT id FROM papers WHERE openalex_id = %s", [paper["openalex_id"]])

    if not existing and paper.get("doi"):
        existing = execute_one(conn, "SELECT id FROM papers WHERE doi = %s", [paper["doi"]])

    if not existing and paper.get("arxiv_id"):
        existing = execute_one(conn, "SELECT id FROM papers WHERE arxiv_id = %s", [paper["arxiv_id"]])

    if existing:
        # Update existing paper
        cols = [k for k in paper.keys() if k != "id"]
        if cols:
            set_clause = ", ".join(f"{c} = %s" for c in cols)
            values = [paper[c] for c in cols] + [existing["id"]]
            execute_write(conn, f"UPDATE papers SET {set_clause} WHERE id = %s", values)
        return existing["id"]
    else:
        # Insert new paper
        cols = list(paper.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)
        values = [paper[c] for c in cols]
        rows = execute_write(conn, f"INSERT INTO papers ({col_names}) VALUES ({placeholders}) RETURNING id", values)
        if rows:
            return rows[0]["id"]

    return None


def get_disciplines_map(conn) -> dict[str, str]:
    """Get mapping of discipline slug -> id."""
    rows = execute(conn, "SELECT id, slug FROM disciplines")
    return {d["slug"]: d["id"] for d in rows}


def get_tags_map(conn) -> dict[str, str]:
    """Get mapping of tag name -> id."""
    rows = execute(conn, "SELECT id, name FROM tags")
    return {t["name"]: t["id"] for t in rows}


def link_paper_to_discipline(conn, paper_id: str, discipline_id: str, source: str = "openalex") -> None:
    """Link a paper to a discipline."""
    try:
        execute_write(conn,
            "INSERT INTO paper_disciplines (paper_id, discipline_id, source) VALUES (%s, %s, %s) ON CONFLICT (paper_id, discipline_id) DO NOTHING",
            [paper_id, discipline_id, source])
    except Exception as e:
        print(f"[DB] Error linking paper to discipline: {e}")
        conn.rollback()


def link_paper_to_tag(conn, paper_id: str, tag_id: str, source: str = "openalex") -> None:
    """Link a paper to a tag."""
    try:
        execute_write(conn,
            "INSERT INTO paper_tags (paper_id, tag_id, source) VALUES (%s, %s, %s) ON CONFLICT (paper_id, tag_id) DO NOTHING",
            [paper_id, tag_id, source])
    except Exception as e:
        print(f"[DB] Error linking paper to tag: {e}")
        conn.rollback()


def get_disciplines(conn) -> list[dict]:
    """Get all disciplines."""
    return execute(conn, "SELECT * FROM disciplines ORDER BY display_order")


def get_recent_papers_by_discipline(conn, discipline_id: str, days: int = 7) -> list[dict]:
    """Get recent papers for a specific discipline."""
    return execute(conn,
        """SELECT p.* FROM paper_disciplines pd
           JOIN papers p ON p.id = pd.paper_id
           WHERE pd.discipline_id = %s AND p.created_at >= NOW() - interval '%s days'""",
        [discipline_id, days])


def get_papers_without_summary(conn, limit: int = 100) -> list[dict]:
    """Get papers that don't have a summary yet."""
    return execute(conn,
        """SELECT p.id, p.title, p.abstract FROM papers p
           LEFT JOIN summaries s ON s.paper_id = p.id
           WHERE s.id IS NULL AND p.abstract IS NOT NULL
           LIMIT %s""",
        [limit])


def save_summary(conn, paper_id: str, summary: dict, model: str) -> None:
    """Save an AI-generated summary."""
    execute_write(conn,
        """INSERT INTO summaries (paper_id, llm_model, so_what, contribution, methodology, data_info, key_finding, limitations)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (paper_id, llm_model) DO UPDATE SET
           so_what = EXCLUDED.so_what, contribution = EXCLUDED.contribution,
           methodology = EXCLUDED.methodology, data_info = EXCLUDED.data_info,
           key_finding = EXCLUDED.key_finding, limitations = EXCLUDED.limitations""",
        [paper_id, model,
         summary.get("so_what"), summary.get("contribution"),
         psycopg2.extras.Json(summary.get("methodology")),
         psycopg2.extras.Json(summary.get("data")),
         summary.get("key_finding"), summary.get("limitations")])


def save_rankings(conn, date: str, discipline_id: str | None, rankings: list[dict]) -> None:
    """Save daily rankings for a discipline."""
    for r in rankings:
        execute_write(conn,
            """INSERT INTO daily_rankings (paper_id, ranking_date, discipline_id, rank_position, score, score_breakdown)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (paper_id, ranking_date, discipline_id) DO UPDATE SET
               rank_position = EXCLUDED.rank_position, score = EXCLUDED.score, score_breakdown = EXCLUDED.score_breakdown""",
            [r["paper_id"], date, discipline_id, r["rank"], r["score"],
             psycopg2.extras.Json(r.get("breakdown"))])


def update_paper_latest_rank(conn, paper_id: str, rank: int, score: float) -> None:
    """Update denormalized rank on papers table."""
    execute_write(conn, "UPDATE papers SET latest_rank = %s, latest_score = %s WHERE id = %s",
                  [rank, score, paper_id])
