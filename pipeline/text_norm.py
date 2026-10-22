"""text_norm.py — strict character-reference decoding, shared by ingest guards and backfill.

`html.unescape` alone is wrong for publisher metadata: it follows the HTML5 list, which
also decodes legacy references with no trailing semicolon. Verified 2026-09-01:

    html.unescape("policy&regulation")  -> "policy®ulation"   (bare "&reg" ate the "reg")
    html.unescape("if&notable")         -> "if¬able"          (bare "&not" ate the "not")
    html.unescape("a&copyright b")      -> "a©right b"        (bare "&copy" ate the "copy")

Each of those inputs is an ordinary ampersand followed by ordinary words ("policy & regulation",
"if & notable", "a & copyright b" with a missing space), not an escaped entity — but the HTML5
legacy list treats "&reg", "&not", "&copy" as complete references and silently eats the letters
that follow. `decode_refs` decodes ONLY well-formed references: numeric (`&#38;`, `&#x26;`) or
named with a trailing semicolon (`&amp;`, `&lt;`), so `R&amp;D` -> `R&D` and `p&lt;0.05` -> `p<0.05`
while `policy&regulation` passes through unchanged. This is the same well-formed definition used to
produce the 2026-09-01 1,632-row measurement in docs/MARKUP_POLICY_WORKORDER.md, so it needs no
restatement there.
"""
from __future__ import annotations

import html
import re

STRICT = re.compile(r'&(#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6}|[a-zA-Z][a-zA-Z0-9]{1,31});')


def decode_refs(s: str) -> str:
    """Decode well-formed HTML character references in `s`; leave everything else untouched.

    `None` in, `None` out (write sites pass optional title/abstract values through unchanged).
    """
    if s is None:
        return None
    return STRICT.sub(lambda m: html.unescape(m.group(0)), s)


# --- Publisher-extraction repairs, measured against the live corpus 2026-09-02 ---
#
# Three defects that DECISIONS 2026-09-01 recorded and this pass re-measured. The
# re-measurement corrected the record in three places, and each correction changed a rule:
#
#   (a) U+21B5 does NOT uniformly stand for a lost "ff" ligature. Of its six occurrences
#       across five rows, four are the ligature ("o↵ers", "e↵ect", "di ↵ er") and two are
#       line breaks ("↵*Corresponding author", "↵† Miguel Sarzosa"). A blanket substitution
#       would corrupt the latter two, so the rule reads its neighbours.
#   (b) The ligature figure 273 was a sum of three per-codepoint row counts, not distinct
#       rows; the true figure is 176 rows, and U+FB03 (ffi) is present although the earlier
#       note listed only FB00/FB01/FB02. More consequentially, 141 of those 176 rows carry a
#       space beside the ligature ("signi ﬁ cant"), so decomposing the character alone leaves
#       most of them unsearchable.
#   (c) The Springer LaTeX block is not a leading preamble but an inline injection, and in
#       all seven spans across four rows the equivalent plain text ("N=45", "91%",
#       "r=.93-1.00", "r-t", an arrow) already precedes it. The span is duplication.
#
# Dry run over all 184 affected rows: every one changed, zero residual defects, and the 156
# that grew are a one-character ligature becoming two letters.

LIGATURES = {
    "ﬀ": "ff",
    "ﬁ": "fi",
    "ﬂ": "fl",
    "ﬃ": "ffi",
    "ﬄ": "ffl",
    "ﬅ": "st",
    "ﬆ": "st",
}

_LIG_CLASS = "".join(LIGATURES)

# Anchored on both ends and non-greedy: a `\documentclass` with no following
# `\end{document}` is left alone rather than eating the rest of the abstract.
_LATEX_SPAN = re.compile(r"\\documentclass.*?\\end\{document\}", re.DOTALL)

# Letters on both sides (one optional space each) means the dropped ligature; anything else
# means the line break the glyph actually denotes.
_RETURN_AS_FF = re.compile(r"(?<=[A-Za-z]) ?\u21b5 ?(?=[A-Za-z])")
_RETURN_AS_BREAK = re.compile(r"\s*\u21b5\s*")

# A space on BOTH sides split one word ("signi ﬁ cant") and the spaces go with the
# decomposition. A space on ONE side is a real word boundary ("and ﬁnance", "staﬀ members")
# and only the character is replaced. All 63 occurrences of the two-sided case were listed
# and read individually on 2026-09-02: no false positives.
_LIG_SPLIT_WORD = re.compile(rf"(?<=[A-Za-z]) ([{_LIG_CLASS}]) (?=[A-Za-z])")
_LIG_ANY = re.compile(rf"[{_LIG_CLASS}]")


def normalize_text(s: str) -> str:
    """Repair the three publisher-extraction defects above; leave everything else alone.

    `None` in, `None` out. Idempotent, unlike decode_refs(): every rule consumes the
    character or span it matches, so a second application is a no-op.

    Runs AFTER decode_refs() at the write choke point. No row in the corpus currently
    carries a ligature or the return symbol as a character reference (checked 2026-09-02,
    zero rows), so that ordering is defensive rather than load-bearing; it costs nothing and
    a publisher emitting `&#64257;` would otherwise slip past this pass.
    """
    if s is None:
        return None
    s = _LATEX_SPAN.sub("", s)
    s = _RETURN_AS_FF.sub("ff", s)
    s = _RETURN_AS_BREAK.sub(" ", s)
    s = _LIG_SPLIT_WORD.sub(lambda m: LIGATURES[m.group(1)], s)
    s = _LIG_ANY.sub(lambda m: LIGATURES[m.group(0)], s)
    return s


# --- Publisher previews posing as abstracts, measured against the live corpus 2026-10-22 ---
#
# Atypon publishers (Taylor & Francis 10.1080, Academy of Management 10.5465) put a 200-character
# preview in <meta name="dc.Description">: the opening of the abstract, or of the body when the
# item has none (a book review, an editorial), cut and ended with "...". OpenAlex and Semantic
# Scholar ingest that meta as the abstract, and so did cdp_drain's meta fallback. Of 195,880
# papers, 1,079 stored one: 1,078 exactly 200 characters, one Elsevier row at 201; none came from
# CrossRef. PREVIEW_SQL is the same test for queries.
PREVIEW_MAX_LEN = 205
PREVIEW_SQL = (r"(length(regexp_replace(abstract, '^\s+|\s+$', '', 'g')) <= 205"
               r" AND abstract ~ '(\.\.\.|…)\s*$')")


def is_truncated_preview(s: str | None) -> bool:
    """True when `s` is a publisher preview, not an abstract: at most 205 characters once
    stripped, ending in "..." or "…". `None` and empty strings are not previews."""
    if not s:
        return False
    t = s.strip()
    return len(t) <= PREVIEW_MAX_LEN and (t.endswith("...") or t.endswith("…"))
