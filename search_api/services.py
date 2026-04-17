from django.db import connection
from typing import List, Dict, Any, Tuple


# =========================================================================== #
#  fuzzystrmatch availability check                                             #
#  levenshtein() requires the fuzzystrmatch extension. On Supabase all         #
#  contrib extensions live in the "extensions" schema. We probe both paths     #
#  once at first use and cache the result.                                      #
# =========================================================================== #

_levenshtein_fn: str | None = None   # None = not yet probed


def _get_levenshtein_fn() -> str:
    """
    Return the SQL callable for levenshtein (qualified if needed), or ''
    if fuzzystrmatch is not installed.  Result is cached after first call.
    """
    global _levenshtein_fn
    if _levenshtein_fn is not None:
        return _levenshtein_fn

    for candidate in ("levenshtein", "extensions.levenshtein"):
        try:
            with connection.cursor() as cur:
                cur.execute(f"SELECT {candidate}('probe', 'probe')")
            _levenshtein_fn = candidate
            return _levenshtein_fn
        except Exception:
            connection.rollback()

    _levenshtein_fn = ''   # unavailable
    return _levenshtein_fn


# =========================================================================== #
#  Court location lookup                                                        #
# =========================================================================== #

def get_court_locations() -> List[str]:
    """Return every distinct, non-null court location, sorted alphabetically."""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT "courtLocation"
            FROM transcripts
            WHERE "courtLocation" IS NOT NULL AND "courtLocation" != ''
            ORDER BY "courtLocation" ASC
        """)
        return [row[0] for row in cursor.fetchall()]


# =========================================================================== #
#  Advanced search – OpenSearch-style permutation scoring                       #
# =========================================================================== #

def advanced_search_transcripts(
    court_location: str,
    style_of_cause: str = '',
    date_range_type: str = '',   # last30 | last60 | last90 | last180 | single | multiple
    single_date: str = '',
    multiple_dates: List[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> Tuple[List[Dict], int]:
    """
    Mandatory field : courtLocation  (hard WHERE filter)
    Optional fields : styleOfCause   (4-layer fuzzy text match)
                      dateRange      (relative or absolute date filter)

    Scoring matrix
    ──────────────────────────────────────────────────────────────────────────
    styleOfCause  dateRange   Formula
    ────────────  ─────────   ─────────────────────────────────────────────────
        ✗             ✗       ORDER BY dateOfTranscript DESC  (browse)
        ✓             ✗       0.65·ts + 0.35·trigram
        ✗             ✓       ORDER BY dateOfTranscript DESC  (filtered browse)
        ✓             ✓       0.55·ts + 0.30·trigram + 0.15·recency_boost
    ──────────────────────────────────────────────────────────────────────────

    Fuzzy layers for styleOfCause (applied in parallel, OR-ed together):
      L1  websearch_to_tsquery  – FTS: stemming, AND/OR/phrase, stop-words
      L2  similarity > 0.10    – whole-string trigram (typos, transpositions)
      L3  word_similarity > 0.15 – partial-word (first/last name fragments)
      L4  ILIKE '%term%'       – exact substring fallback for short queries

    recency_boost = 1 / (1 + days_old / 60)   [half-life 60 days, max 1.0]
    """
    if not court_location:
        return [], 0

    multiple_dates = multiple_dates or []
    has_text = bool(style_of_cause.strip())
    has_date = bool(date_range_type)

    params: list = []

    # ------------------------------------------------------------------ #
    # SELECT + score columns                                               #
    # ------------------------------------------------------------------ #
    if has_text:
        soc = style_of_cause
        lev_fn = _get_levenshtein_fn()
        if lev_fn:
            # Per-word edit-distance: 1 − (distance / max_len), best word wins.
            # Supabase places fuzzystrmatch in the "extensions" schema, so we use
            # the probed qualified name (_get_levenshtein_fn returns the right one).
            edit_score_expr = f"""COALESCE((
                        SELECT MAX(
                            1.0 - (
                                {lev_fn}(lower(unaccent(%s)), lower(unaccent(word)))::float
                                / NULLIF(GREATEST(length(%s), length(word)), 0)
                            )
                        )
                        FROM regexp_split_to_table(unaccent("styleOfCause"), '\\s+') AS word
                        WHERE length(word) >= 2
                    ), 0.0)"""
            edit_params = [soc, soc]   # two extra positional params for the subquery
        else:
            edit_score_expr = "0.0"
            edit_params = []

        sql = """
            WITH search_results AS (
                SELECT
                    id, "ticketId", "styleOfCause", "dateOfTranscript",
                    "courtLocation", "presidingOfficial", "totalPages", verified,

                    -- FTS scoped to styleOfCause ONLY.
                    -- Using the full search_vector (which includes portionTyped at weight C)
                    -- caused unrelated transcripts to rank higher when the query term
                    -- appeared anywhere in the transcript body. Scoping to the field
                    -- ensures only the case name itself drives the FTS score.
                    ts_rank_cd(
                        to_tsvector('english', unaccent("styleOfCause")),
                        websearch_to_tsquery('english', unaccent(%s))
                    ) AS ts_score,

                    -- Trigram score: best of full-string and word-level similarity.
                    GREATEST(
                        similarity(unaccent("styleOfCause"), unaccent(%s)),
                        word_similarity(unaccent(%s), unaccent("styleOfCause"))
                    ) AS trigram_score,

                    EDIT_SCORE_PLACEHOLDER AS edit_score,

                    GREATEST(0.0,
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - "dateOfTranscript")) / 86400.0
                    ) AS days_old,

                    ts_headline(
                        'english',
                        coalesce("portionTyped", '') || ' ' || coalesce("actName", ''),
                        websearch_to_tsquery('english', unaccent(%s)),
                        'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=12'
                    ) AS context_snippet

                FROM transcripts
                WHERE
                    "courtLocation" = %s
                    AND (
                        -- FTS on styleOfCause field only (not full search_vector)
                        to_tsvector('english', unaccent("styleOfCause"))
                            @@ websearch_to_tsquery('english', unaccent(%s))
                        OR similarity(unaccent("styleOfCause"), unaccent(%s)) > 0.10
                        OR word_similarity(unaccent(%s), unaccent("styleOfCause")) > 0.15
                        OR unaccent("styleOfCause") ILIKE '%%' || unaccent(%s) || '%%'
                    )
        """
        # Replace the EDIT_SCORE_PLACEHOLDER token with the actual SQL expression.
        # We use a simple str.replace rather than .format() to avoid escaping all
        # the %s parameter markers in the rest of the query.
        sql = sql.replace("EDIT_SCORE_PLACEHOLDER", edit_score_expr)

        # Params: ts_rank(1) + trigram(2) + edit_score(0 or 2) + ts_headline(1)
        #         + courtLocation(1) + WHERE(4) = 9 or 11 total
        params = (
            [soc]               # ts_rank
            + [soc, soc]        # trigram similarity + word_similarity
            + edit_params       # levenshtein query × 2, or empty []
            + [soc]             # ts_headline
            + [court_location]  # WHERE courtLocation
            + [soc, soc, soc, soc]  # WHERE fuzzy conditions
        )
    else:
        sql = """
            WITH search_results AS (
                SELECT
                    id, "ticketId", "styleOfCause", "dateOfTranscript",
                    "courtLocation", "presidingOfficial", "totalPages", verified,
                    0.0 AS ts_score,
                    0.0 AS trigram_score,
                    0.0 AS edit_score,
                    0.0 AS days_old,
                    ''  AS context_snippet
                FROM transcripts
                WHERE "courtLocation" = %s
        """
        params = [court_location]

    # ------------------------------------------------------------------ #
    # Date range filter                                                    #
    # ------------------------------------------------------------------ #
    _interval_map = {
        'last30':  '30 days',
        'last60':  '60 days',
        'last90':  '90 days',
        'last180': '180 days',
    }
    if date_range_type in _interval_map:
        sql += f" AND \"dateOfTranscript\" >= CURRENT_DATE - INTERVAL '{_interval_map[date_range_type]}'"
    elif date_range_type == 'single' and single_date:
        sql += ' AND "dateOfTranscript"::date = %s'
        params.append(single_date)
    elif date_range_type == 'multiple' and multiple_dates:
        valid = [d for d in multiple_dates if d]
        if valid:
            placeholders = ', '.join(['%s'] * len(valid))
            sql += f' AND "dateOfTranscript"::date IN ({placeholders})'
            params.extend(valid)

    # ------------------------------------------------------------------ #
    # Final score expression (depends on which fields are active)          #
    # ------------------------------------------------------------------ #
    #
    # Weights when styleOfCause is provided:
    #   ts_score     0.45  – FTS on styleOfCause field (stemming, exact lexeme)
    #   trigram_score 0.25 – pg_trgm word_similarity / similarity
    #   edit_score    0.30 – normalised Levenshtein per word (phonetic differentiator)
    #
    # edit_score correctly separates names like "jasbir" (edit dist 3 from "jasprit")
    # vs "jassy" (edit dist 4), which score identically on trigrams alone.
    #
    if has_text and has_date:
        score_expr = (
            "(0.40 * ts_score)"
            " + (0.22 * trigram_score)"
            " + (0.25 * edit_score)"
            " + (0.13 * (1.0 / (1.0 + days_old / 60.0)))"
        )
    elif has_text:
        score_expr = "(0.45 * ts_score) + (0.25 * trigram_score) + (0.30 * edit_score)"
    else:
        score_expr = "0.0"

    sql += f"""
            )
            SELECT *,
                   {score_expr} AS final_score
            FROM search_results
            ORDER BY final_score DESC, "dateOfTranscript" DESC
    """

    count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _counted"
    paged_sql = sql + " LIMIT %s OFFSET %s"

    with connection.cursor() as cursor:
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()[0]

        cursor.execute(paged_sql, params + [limit, offset])
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return results, total_count


# =========================================================================== #
#  Original broad search (kept for backward compatibility)                      #
# =========================================================================== #

def search_transcripts(
    query: str, limit: int = 20, offset: int = 0, filters: Dict[str, Any] = None
) -> Tuple[List[Dict], int]:
    """
    Returns (results, total_count).

    Fuzzy matching strategy (OpenSearch-style, layered):
      1. Full-text tsquery  — handles stemming, stop words, AND/OR/phrase
      2. word_similarity    — partial-word fuzzy (query word appears inside a field value)
      3. similarity         — whole-string similarity (handles transpositions / typos)
      4. ILIKE              — simple substring fallback for very short terms

    Thresholds are intentionally low (0.10–0.20) so that minor typos and
    partial names still surface relevant records.
    """
    filters = filters or {}

    # With no text query and no filters there is nothing to search.
    if not query and not filters:
        return [], 0

    params: list = []

    # ------------------------------------------------------------------ #
    # Build the inner CTE                                                  #
    # ------------------------------------------------------------------ #
    if query:
        sql = """
            WITH search_results AS (
                SELECT
                    id, "ticketId", "styleOfCause", "dateOfTranscript",
                    "courtLocation", "presidingOfficial", "totalPages", verified,

                    -- Full-text ranking (0 when tsquery doesn't match)
                    ts_rank_cd(
                        search_vector,
                        websearch_to_tsquery('english', unaccent(%s))
                    ) AS ts_score,

                    -- Best trigram signal across all searchable text fields
                    GREATEST(
                        similarity(unaccent("styleOfCause"), unaccent(%s)),
                        word_similarity(unaccent(%s), unaccent("styleOfCause")),
                        COALESCE(word_similarity(unaccent(%s), unaccent("presidingOfficial")), 0),
                        COALESCE(word_similarity(unaccent(%s), unaccent("courtLocation")), 0),
                        COALESCE(word_similarity(unaccent(%s), unaccent("ticketId")), 0)
                    ) AS trigram_score,

                    ts_headline(
                        'english',
                        coalesce("portionTyped", '') || ' ' || coalesce("actName", ''),
                        websearch_to_tsquery('english', unaccent(%s)),
                        'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=12'
                    ) AS context_snippet

                FROM transcripts
                WHERE (
                    -- Layer 1: full-text match
                    search_vector @@ websearch_to_tsquery('english', unaccent(%s))

                    -- Layer 2: whole-string trigram similarity (catches typos)
                    OR similarity(unaccent("styleOfCause"), unaccent(%s)) > 0.10

                    -- Layer 3: word-level similarity (catches partial names / first-word matches)
                    OR word_similarity(unaccent(%s), unaccent("styleOfCause")) > 0.15
                    OR word_similarity(unaccent(%s), unaccent(coalesce("presidingOfficial", ''))) > 0.20
                    OR word_similarity(unaccent(%s), unaccent(coalesce("courtLocation", ''))) > 0.20
                    OR word_similarity(unaccent(%s), unaccent(coalesce("ticketId", ''))) > 0.20

                    -- Layer 4: simple substring fallback for short / partial terms
                    OR unaccent("styleOfCause") ILIKE '%%' || unaccent(%s) || '%%'
                    OR unaccent(coalesce("presidingOfficial", '')) ILIKE '%%' || unaccent(%s) || '%%'
                    OR unaccent(coalesce("ticketId", '')) ILIKE '%%' || unaccent(%s) || '%%'
                )
        """
        # params order: ts_rank×1, similarity×1, word_sim×5, ts_headline×1,
        #               tsquery WHERE×1, similarity WHERE×1, word_sim WHERE×5,
        #               ILIKE WHERE×3  = 18 query params total
        params = [
            query, query, query, query, query, query,   # score cols
            query,                                       # ts_headline
            query, query, query, query, query, query,   # WHERE layers 1-3
            query, query, query,                        # WHERE layer 4
        ]
    else:
        # Filter-only search (no text query)
        sql = """
            WITH search_results AS (
                SELECT
                    id, "ticketId", "styleOfCause", "dateOfTranscript",
                    "courtLocation", "presidingOfficial", "totalPages", verified,
                    0.0 AS ts_score,
                    0.0 AS trigram_score,
                    '' AS context_snippet
                FROM transcripts
                WHERE 1=1
        """

    # ------------------------------------------------------------------ #
    # Append filter conditions                                             #
    # ------------------------------------------------------------------ #
    if filters.get('courtLocation'):
        sql += ' AND "courtLocation" = %s'
        params.append(filters['courtLocation'])

    if filters.get('verified') is not None:
        sql += ' AND verified = %s'
        params.append(int(filters['verified']))

    if filters.get('startDate') and filters.get('endDate'):
        sql += ' AND "dateOfTranscript" BETWEEN %s AND %s'
        params.extend([filters['startDate'], filters['endDate']])

    for ban_type in ['banYcja', 'banSealedRecording', 'banPublication', 'banNoPublication']:
        if filters.get(ban_type) == 'true':
            sql += f' AND "{ban_type}" = true'

    # ------------------------------------------------------------------ #
    # Final SELECT with ranking                                            #
    # ------------------------------------------------------------------ #
    sql += """
            )
            SELECT *,
                   (0.65 * ts_score) + (0.35 * trigram_score) AS final_score
            FROM search_results
            ORDER BY final_score DESC, "dateOfTranscript" DESC
    """

    # Count total before paging
    count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _counted"
    paged_sql = sql + " LIMIT %s OFFSET %s"

    with connection.cursor() as cursor:
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()[0]

        cursor.execute(paged_sql, params + [limit, offset])
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    return results, total_count