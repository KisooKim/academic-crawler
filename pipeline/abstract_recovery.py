"""Free-layer abstract recovery (self-contained; runs on public GHA).

For papers ingested without an abstract, recover from the free, no-browser,
no-Cloudflare layers in order:

    Semantic Scholar /paper/batch  →  CrossRef  →  Unpaywall

(OpenAlex is already tried inline at ingest, so it is not repeated here.)

Self-contained on purpose: depends only on this repo's db.py + httpx/requests +
stdlib, so it works in the academic-crawler public GitHub Actions runner. The
S2 / CrossRef / Unpaywall / validation logic mirrors the proven implementations
in the LiterView `classifier/abstract_recovery/` R&D tree.

Papers still missing after the free layers get an `abstract_recovery_state` so
the sweep never re-processes them and the Cloudflare residual is queued for the
Home CDP drain:

    free_exhausted  free layers failed, Cloudflare publisher → Home CDP queue
    free_dead       free layers failed, non-Cloudflare → no automated path
    cdp_exhausted   (set by the CDP drain / seed) confirmed no abstract
"""
from __future__ import annotations

import re
import time
from collections import Counter
from urllib.parse import quote

import httpx
import requests

from db import execute_write

# ── DOI prefixes that sit behind Cloudflare and need the Home CDP drain ──────
# (from LiterView run_l5_unified PUBLISHERS). Nature/Springer (10.1038/10.1007)
# are headless-friendly but blocked from datacenter IPs, so they fall to
# free_dead here rather than a GHA browser attempt.
CLOUDFLARE_PREFIXES = {
    "10.1016",          # Elsevier (the main recoverable Cloudflare publisher)
    "10.1111", "10.1002",  # Wiley
    "10.1086",          # UChicago
    "10.1017",          # Cambridge
    "10.1093",          # Oxford
    "10.1177",          # SAGE (mostly no-abstract types; drain applies triage)
    "10.1080", "10.4324",  # Taylor & Francis / Routledge
}

# How long to keep retrying the free layers before giving up (aggregator
# indexing lag means a brand-new paper may not be in S2/CrossRef for days).
GIVE_UP_AFTER = "30 days"
RETRY_EVERY = "2 days"

S2_BATCH_URL = "https://api.semanticscholar.org/graph/v1/paper/batch"
CROSSREF_URL = "https://api.crossref.org/works/"
UNPAYWALL_URL = "https://api.unpaywall.org/v2/"
USER_AGENT = "academic-crawler-abstract-recovery/1.0 (mailto:{email})"

UPDATE_SQL = """
UPDATE papers
SET abstract = %s, abstract_source = %s, updated_at = NOW()
WHERE id = %s
  AND (abstract IS NULL OR LENGTH(TRIM(abstract)) < 50)
"""
# Mark the residual: keep retrying recent papers ('pending'); only give up
# (terminal state, %s = free_exhausted / free_dead) once the paper has aged out.
STATE_SQL = f"""
UPDATE papers
SET abstract_recovery_state = CASE
        WHEN created_at < NOW() - INTERVAL '{GIVE_UP_AFTER}' THEN %s
        ELSE 'pending' END,
    abstract_recovery_last = NOW()
WHERE id = %s AND abstract IS NULL
"""

_JATS = re.compile(r"</?jats:[^>]+>", re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")


# ── helpers (vendored from LiterView pilot/common.py + run_layer2_crossref) ──
def normalize_doi(doi: str | None) -> str | None:
    if not doi:
        return None
    s = doi.strip()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if s.lower().startswith(prefix):
            s = s[len(prefix):]
    return s.lower().strip()


def clean_jats(text: str) -> str:
    if not text:
        return text
    return _TAG.sub("", _JATS.sub("", text)).strip()


def is_valid_abstract(text: str | None, title: str | None = None) -> tuple[bool, str]:
    if not text:
        return False, "empty"
    t = text.strip()
    if len(t) < 100:
        return False, f"too_short:{len(t)}"
    if len(t) > 12000:
        return False, f"too_long:{len(t)}"
    low = t.lower()
    for bad in ("no abstract available", "abstract not available",
                "copyright ©", "all rights reserved"):
        if low.startswith(bad):
            return False, f"boilerplate:{bad[:20]}"
    if title and low.startswith(title.lower()[:80]) and len(t) < len(title) + 120:
        return False, "title_only"
    return True, ""


def state_for(doi: str) -> str:
    prefix = doi.split("/", 1)[0] if doi else ""
    return "free_exhausted" if prefix in CLOUDFLARE_PREFIXES else "free_dead"


# ── layer fetchers ──────────────────────────────────────────────────────────
def s2_batch(dois: list[str], keys: list[str], rate: float = 1.1,
             timeout: float = 30.0) -> dict[str, str]:
    """Return {normalized_doi: abstract} for DOIs S2 knows. 500 ids/request."""
    out: dict[str, str] = {}
    ids = [f"DOI:{d}" for d in dois]
    key_idx = 0
    i = 0
    while i < len(ids):
        chunk = ids[i:i + 500]
        headers = {"x-api-key": keys[key_idx]} if keys else {}
        try:
            r = requests.post(S2_BATCH_URL, params={"fields": "externalIds,abstract"},
                              json={"ids": chunk}, headers=headers, timeout=timeout)
        except Exception:
            i += 500
            time.sleep(rate)
            continue
        if r.status_code == 429 and keys and len(keys) > 1:
            key_idx = (key_idx + 1) % len(keys)
            time.sleep(rate)
            continue                      # retry same chunk with next key
        if r.status_code != 200:
            i += 500
            time.sleep(rate)
            continue
        try:
            results = r.json()
        except Exception:
            i += 500
            time.sleep(rate)
            continue
        for p in results or []:
            if not p:
                continue
            ext = p.get("externalIds") or {}
            d = normalize_doi(ext.get("DOI") or p.get("doi"))
            ab = p.get("abstract")
            if d and ab:
                out[d] = ab
        i += 500
        time.sleep(rate)
    return out


def fetch_crossref(client: httpx.Client, doi: str) -> str | None:
    try:
        r = client.get(CROSSREF_URL + quote(doi, safe="/"))
    except Exception:
        return None
    if r.status_code != 200:
        return None
    try:
        raw = r.json().get("message", {}).get("abstract")
    except Exception:
        return None
    return clean_jats(raw) if raw else None


def fetch_unpaywall(client: httpx.Client, doi: str, email: str) -> str | None:
    try:
        r = client.get(f"{UNPAYWALL_URL}{quote(doi, safe='/')}?email={email}")
    except Exception:
        return None
    if r.status_code != 200:
        return None
    try:
        raw = r.json().get("abstract")
    except Exception:
        return None
    return clean_jats(raw) if raw else None


# ── orchestration ───────────────────────────────────────────────────────────
def recover_batch(conn, papers: list[dict], *, s2_keys: list[str] | None = None,
                  email: str | None = None, dry_run: bool = False,
                  set_state: bool = True, s2_rate: float = 1.1,
                  rest_rate: float = 0.34) -> dict:
    """papers: [{id, doi, title}]. Recover via S2 → CrossRef → Unpaywall,
    write abstracts, and mark the residual with abstract_recovery_state."""
    by_doi: dict[str, dict] = {}
    for p in papers:
        d = normalize_doi(p.get("doi"))
        if d:
            by_doi[d] = {"id": p["id"], "title": p.get("title") or ""}

    stats: Counter = Counter()
    done: set[str] = set()

    def _write(doi: str, abstract: str, source: str) -> bool:
        ok, why = is_valid_abstract(abstract, by_doi[doi]["title"])
        if not ok:
            stats[f"invalid_{source}"] += 1
            return False
        if not dry_run:
            execute_write(conn, UPDATE_SQL, [abstract, source, by_doi[doi]["id"]])
        done.add(doi)
        stats[f"recovered_{source}"] += 1
        return True

    # Layer 1: Semantic Scholar batch
    if by_doi:
        for doi, ab in s2_batch(list(by_doi), s2_keys or [], rate=s2_rate).items():
            if doi in by_doi:
                _write(doi, ab, "semantic_scholar")

    # Layer 2/3: CrossRef → Unpaywall for the residual
    residual = [d for d in by_doi if d not in done]
    if residual:
        ua = USER_AGENT.format(email=email or "")
        with httpx.Client(headers={"User-Agent": ua, "Accept": "application/json"},
                          timeout=30.0, follow_redirects=True) as client:
            for doi in residual:
                ab = fetch_crossref(client, doi)
                src = "crossref"
                if not ab and email:
                    ab = fetch_unpaywall(client, doi, email)
                    src = "unpaywall"
                if ab:
                    _write(doi, ab, src)
                time.sleep(rest_rate)

    # Mark residual: recent papers -> 'pending' (retried later); aged-out papers
    # -> terminal route (free_exhausted for Cloudflare = Tier-2 CDP queue, else
    # free_dead). The CASE in STATE_SQL decides pending vs terminal by age.
    if set_state:
        for doi in by_doi:
            if doi in done:
                continue
            stats["residual"] += 1
            stats[f"route_{state_for(doi)}"] += 1
            if not dry_run:
                execute_write(conn, STATE_SQL, [state_for(doi), by_doi[doi]["id"]])

    stats["processed"] = len(by_doi)
    return dict(stats)
