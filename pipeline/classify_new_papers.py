"""
classify_new_papers.py — Classify papers that have no subfield tags yet.

Designed for CI: fetches unclassified papers from Supabase, applies
keyword matching + centroid-embedding classification, uploads results.

Usage:
  python classify_new_papers.py                  # classify all untagged papers
  python classify_new_papers.py --limit 500      # process at most N papers
  python classify_new_papers.py --threshold 0.35 # stricter embedding threshold
"""
import os
import re
import sys
import time
import json
import numpy as np
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

PIPELINE_DIR = Path(__file__).parent
load_dotenv(dotenv_path=PIPELINE_DIR.parent / ".env.local")

CENTROIDS_FILE = PIPELINE_DIR / "subfield_centroids_claude.json"
THRESHOLDS_FILE = PIPELINE_DIR / "subfield_thresholds.json"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
DEFAULT_THRESHOLD = 0.30
MAX_SUBFIELDS = 3
BATCH_SIZE = 200


def get_client() -> Client:
    url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def get_tags_map(client: Client) -> dict:
    r = client.table("tags").select("id, name").eq("type", "subfield").execute()
    return {t["name"]: t["id"] for t in r.data} if r.data else {}


def get_unclassified_papers(client: Client, limit: int = None) -> list:
    """
    Fetch papers that have no paper_tags rows at all.
    Uses a NOT EXISTS approach via Supabase RPC or pagination filter.
    """
    print("[Classify] Fetching papers already tagged...")
    tagged_ids = set()
    offset = 0
    while True:
        r = client.table("paper_tags").select("paper_id").range(offset, offset + 999).execute()
        batch = r.data or []
        if not batch:
            break
        for row in batch:
            tagged_ids.add(row["paper_id"])
        if len(batch) < 1000:
            break
        offset += 1000
    print(f"  {len(tagged_ids)} papers already have tags")

    print("[Classify] Fetching all papers...")
    papers = []
    offset = 0
    while True:
        r = client.table("papers").select("id, title, source, abstract").range(offset, offset + 999).execute()
        batch = r.data or []
        if not batch:
            break
        for p in batch:
            if p["id"] not in tagged_ids:
                papers.append(p)
        if len(batch) < 1000:
            break
        offset += 1000
        if limit and len(papers) >= limit * 3:  # fetch enough to find limit untagged
            break

    if limit:
        papers = papers[:limit]

    print(f"  {len(papers)} unclassified papers to process")
    return papers


def load_centroids() -> dict:
    if not CENTROIDS_FILE.exists():
        raise FileNotFoundError(
            f"Centroids file not found: {CENTROIDS_FILE}\n"
            "Run reclassify_no_abstract.py first to build centroids."
        )
    with open(CENTROIDS_FILE) as f:
        centroids = json.load(f)
    for sf in centroids:
        centroids[sf]["centroid"] = np.array(centroids[sf]["centroid"])
    print(f"[Classify] Loaded {len(centroids)} subfield centroids")
    return centroids


def load_thresholds() -> dict:
    if not THRESHOLDS_FILE.exists():
        return {}
    with open(THRESHOLDS_FILE) as f:
        thresholds = json.load(f)
    print(f"[Classify] Loaded per-subfield thresholds for {len(thresholds)} subfields")
    return thresholds


def keyword_classify(title: str, abstract: str = "", source: str = "") -> list:
    from subfield_keywords import SUBFIELD_KEYWORDS
    text = (title + " " + (abstract or "") + " " + (source or "")).lower()
    matches = []
    for _, subfields in SUBFIELD_KEYWORDS.items():
        for sf_slug, info in subfields.items():
            count = sum(
                1 for kw in info["keywords"]
                if re.search(r'\b' + re.escape(kw.lower()) + r'(?:e?s)?\b', text)
            )
            if count >= 2:
                matches.append((sf_slug, min(0.9, 0.5 + count * 0.1)))
    matches.sort(key=lambda x: x[1], reverse=True)
    return matches[:MAX_SUBFIELDS]


def embedding_classify(model, centroids: dict, title: str, default_threshold: float, thresholds: dict = None) -> list:
    text = (title or "").strip()
    if not text:
        return []
    emb = model.encode(text, show_progress_bar=False)
    emb = emb / np.linalg.norm(emb)
    sims = [(sf, float(np.dot(emb, data["centroid"]))) for sf, data in centroids.items()]
    sims.sort(key=lambda x: x[1], reverse=True)
    results = []
    for sf, score in sims:
        cutoff = thresholds.get(sf, default_threshold) if thresholds else default_threshold
        if score >= cutoff:
            results.append((sf, score))
    return results[:MAX_SUBFIELDS]


def flush_batch(client: Client, batch: list) -> Client:
    if not batch:
        return client
    for attempt in range(3):
        try:
            client.table("paper_tags").upsert(batch, on_conflict="paper_id,tag_id").execute()
            return client
        except Exception as e:
            if attempt < 2:
                print(f"  Retry {attempt+1}: {e}")
                time.sleep(2 * (attempt + 1))
                client = get_client()
            else:
                print(f"  Failed: {e}")
    return client


def main():
    threshold = DEFAULT_THRESHOLD
    limit = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--threshold" and i < len(sys.argv) - 1:
            threshold = float(sys.argv[i + 1])
        elif arg == "--limit" and i < len(sys.argv) - 1:
            limit = int(sys.argv[i + 1])

    print(f"[Classify] Starting at {datetime.now().isoformat()}")
    print(f"  Threshold: {threshold}  |  Limit: {limit or 'all'}")

    client = get_client()
    tags_map = get_tags_map(client)
    centroids = load_centroids()
    thresholds = load_thresholds()

    print("[Classify] Loading embedding model...")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(EMBEDDING_MODEL)

    papers = get_unclassified_papers(client, limit)
    if not papers:
        print("[Classify] No unclassified papers found. Done.")
        return

    stats = defaultdict(int)
    upsert_batch = []

    for i, paper in enumerate(papers):
        paper_id = paper["id"]
        title = paper.get("title", "") or ""
        abstract = paper.get("abstract", "") or ""
        source = paper.get("source", "") or ""

        kw = keyword_classify(title, abstract, source)
        emb = embedding_classify(model, centroids, title, threshold, thresholds)

        assigned = {}
        for sf, conf in kw:
            assigned[sf] = ("keyword", conf)
        for sf, score in emb:
            if sf not in assigned:
                assigned[sf] = ("embedding", score)
            if len(assigned) >= MAX_SUBFIELDS:
                break

        if assigned:
            stats["classified"] += 1
            for sf, (src, conf) in list(assigned.items())[:MAX_SUBFIELDS]:
                tag_id = tags_map.get(sf)
                if tag_id:
                    upsert_batch.append({
                        "paper_id": paper_id,
                        "tag_id": tag_id,
                        "confidence": conf,
                        "source": src,
                    })
        else:
            stats["unclassified"] += 1

        if len(upsert_batch) >= BATCH_SIZE:
            client = flush_batch(client, upsert_batch)
            upsert_batch = []

        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(papers)} processed...")

    if upsert_batch:
        client = flush_batch(client, upsert_batch)

    total = len(papers)
    print("\n" + "=" * 50)
    print(f"Total processed:  {total}")
    print(f"Classified:       {stats['classified']} ({100*stats['classified']//max(1,total)}%)")
    print(f"Unclassified:     {stats['unclassified']}")
    print(f"[Classify] Done at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
