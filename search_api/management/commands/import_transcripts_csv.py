import csv
import uuid
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.utils import timezone


# Internal field -> possible CSV column names (camelCase / Transcript Finder aliases).
INTERNAL_ALIASES: dict[str, tuple[str, ...]] = {
    "ticketId": ("ticketId", "TICKET ID"),
    "styleOfCause": ("styleOfCause", "STYLE OF CAUSE / MATTER NAME"),
    "dateOfTranscript": ("dateOfTranscript", "DATE OF TRANSCRIPT"),
    "courtLocation": ("courtLocation", "COURT LOCATION"),
    "portionTyped": ("portionTyped", "PORTION TYPED"),
    "presidingOfficial": ("presidingOfficial", "PRESIDING OFFICIAL"),
    "courtroomNumber": ("courtroomNumber", "COURTROOM NUMBER"),
    "publicationBansRaw": ("publicationBansRaw", "PUBLICATION BANS"),
    "actName": ("actName", "ACT NAME"),
    "actEmailAddress": ("actEmailAddress", "ACT EMAIL ADDRESS"),
    "totalPages": ("totalPages", "TOTAL PAGES"),
    "companyName": ("companyName", "COMPANY NAME"),
    "banYcja": ("banYcja",),
    "banSealedRecording": ("banSealedRecording",),
    "banPublication": ("banPublication",),
    "banNoPublication": ("banNoPublication",),
    "views": ("views",),
    "verified": ("verified",),
}

# Minimum columns needed to build a row (aliases resolved per file).
REQUIRED_INTERNAL = ("styleOfCause", "dateOfTranscript")

INSERT_SQL = """
INSERT INTO transcripts (
    "id", "ticketId", "styleOfCause", "dateOfTranscript",
    "courtLocation", "portionTyped", "presidingOfficial", "courtroomNumber",
    "publicationBansRaw", "actName", "actEmailAddress", "totalPages", "companyName",
    "banYcja", "banSealedRecording", "banPublication", "banNoPublication",
    "views", "verified", "createdAt", "updatedAt"
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s
)
"""


def _empty_to_none(s: str | None) -> str | None:
    if s is None:
        return None
    t = s.strip()
    return t if t else None


def _parse_datetime_cell(s: str) -> datetime:
    """Parse Transcript Finder DD-MM-YYYY, DD-MM-YYYY HH:MM, ISO-ish exports, etc."""
    s = (s or "").strip()
    if not s:
        raise ValueError("empty date")
    tz = timezone.get_current_timezone()
    if s.endswith("Z") and "T" in s.upper():
        s2 = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = timezone.make_aware(dt, tz)
            else:
                dt = dt.astimezone(tz)
            return dt
        except ValueError:
            pass
    for fmt in ("%d-%m-%Y %H:%M", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return timezone.make_aware(dt, tz)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return timezone.make_aware(dt, tz)
        return dt.astimezone(tz)
    except ValueError as e:
        raise ValueError(f"unrecognized date: {s!r}") from e


def _parse_int(s: str | None) -> int | None:
    s = _empty_to_none(s)
    if s is None:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _parse_publication_ban(raw: str | None) -> tuple[str | None, bool]:
    """Store raw text; YES => banPublication True (per Ontario transcript listing)."""
    raw = _empty_to_none(raw)
    if raw is None:
        return None, False
    upper = raw.upper()
    if upper in ("YES", "Y", "TRUE", "1"):
        return raw, True
    return raw, False


def _parse_bool_cell(val: str | None, default: bool = False) -> bool:
    if val is None:
        return default
    u = str(val).strip().upper()
    if u in ("TRUE", "1", "YES", "T"):
        return True
    if u in ("FALSE", "0", "NO", "F"):
        return False
    return default


def _build_column_map(fieldnames: list[str]) -> dict[str, str]:
    """Map internal field name -> actual CSV header present in this file."""
    fn = set(fieldnames)
    colmap: dict[str, str] = {}
    for internal, aliases in INTERNAL_ALIASES.items():
        for alias in aliases:
            if alias in fn:
                colmap[internal] = alias
                break
    missing = [k for k in REQUIRED_INTERNAL if k not in colmap]
    if missing:
        raise CommandError(
            f"CSV is missing required columns (as Transcript Finder or camelCase headers): {missing}. "
            f"Found: {fieldnames}"
        )
    return colmap


def _cell(row: dict, colmap: dict[str, str], internal: str) -> str | None:
    h = colmap.get(internal)
    if not h:
        return None
    v = row.get(h)
    return v if v is None else str(v)


def _resolve_ban_fields(
    row: dict, colmap: dict[str, str]
) -> tuple[str | None, bool, bool, bool, bool]:
    """Returns publicationBansRaw, banYcja, banSealedRecording, banPublication, banNoPublication."""
    raw = _empty_to_none(_cell(row, colmap, "publicationBansRaw"))
    explicit = all(
        k in colmap
        for k in ("banYcja", "banSealedRecording", "banPublication", "banNoPublication")
    )
    if explicit:
        return (
            raw,
            _parse_bool_cell(_cell(row, colmap, "banYcja")),
            _parse_bool_cell(_cell(row, colmap, "banSealedRecording")),
            _parse_bool_cell(_cell(row, colmap, "banPublication")),
            _parse_bool_cell(_cell(row, colmap, "banNoPublication")),
        )
    _, ban_pub = _parse_publication_ban(raw)
    return raw, False, False, ban_pub, False


class Command(BaseCommand):
    help = (
        "Import transcript rows into the transcripts table. "
        "Accepts Transcript Finder column names (e.g. TICKET ID) or Prisma/camelCase export "
        "(e.g. ticketId, styleOfCause)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "csv_path",
            nargs="?",
            type=str,
            default=None,
            help="Path to CSV file (default: backend/transcript.csv)",
        )
        parser.add_argument(
            "--clear",
            action="store_true",
            help="DELETE all rows from transcripts before import.",
        )
        parser.add_argument(
            "--strict",
            action="store_true",
            help=(
                "Abort if any row fails validation. "
                "Default: skip bad rows (e.g. unparseable dates) and import the rest."
            ),
        )

    def handle(self, *args, **options):
        backend_dir = Path(__file__).resolve().parents[3]
        default_csv = backend_dir / "transcript.csv"
        path = Path(options["csv_path"] or default_csv)
        if not path.is_file():
            raise CommandError(f"CSV not found: {path}")

        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise CommandError("CSV has no header row.")
            colmap = _build_column_map(list(reader.fieldnames))
            rows = list(reader)

        now = timezone.now()
        batch: list[tuple] = []
        strict = options["strict"]
        errors: list[str] = []
        skipped: list[str] = []

        def _fail(msg: str) -> None:
            if strict:
                errors.append(msg)
            else:
                skipped.append(msg)

        for i, row in enumerate(rows, start=2):
            try:
                style = _empty_to_none(_cell(row, colmap, "styleOfCause"))
                if not style:
                    _fail(f"line {i}: missing style of cause")
                    continue

                dt = _parse_datetime_cell(_cell(row, colmap, "dateOfTranscript") or "")
                pub_raw, ban_y, ban_sr, ban_pub, ban_np = _resolve_ban_fields(row, colmap)

                views = _parse_int(_cell(row, colmap, "views"))
                if views is None:
                    views = 0
                verified = _parse_int(_cell(row, colmap, "verified"))
                if verified is None:
                    verified = 0

                co = _empty_to_none(_cell(row, colmap, "companyName"))
                if not co:
                    co = "VIDEOPLUS"

                batch.append(
                    (
                        str(uuid.uuid4()),
                        _empty_to_none(_cell(row, colmap, "ticketId")),
                        style,
                        dt,
                        _empty_to_none(_cell(row, colmap, "courtLocation")),
                        _empty_to_none(_cell(row, colmap, "portionTyped")),
                        _empty_to_none(_cell(row, colmap, "presidingOfficial")),
                        _empty_to_none(_cell(row, colmap, "courtroomNumber")),
                        pub_raw,
                        _empty_to_none(_cell(row, colmap, "actName")),
                        _empty_to_none(_cell(row, colmap, "actEmailAddress")),
                        _parse_int(_cell(row, colmap, "totalPages")),
                        co,
                        ban_y,
                        ban_sr,
                        ban_pub,
                        ban_np,
                        views,
                        verified,
                        now,
                        now,
                    )
                )
            except Exception as e:
                _fail(f"line {i}: {e}")

        if strict and errors:
            for msg in errors[:20]:
                self.stderr.write(self.style.WARNING(msg))
            if len(errors) > 20:
                self.stderr.write(self.style.WARNING(f"... and {len(errors) - 20} more errors"))
            raise CommandError(f"Aborted: {len(errors)} row(s) failed validation.")

        if not batch:
            raise CommandError(
                f"No valid rows to insert ({len(skipped)} skipped)." if skipped else "No rows to insert."
            )

        if skipped:
            for msg in skipped[:20]:
                self.stderr.write(self.style.WARNING(msg))
            if len(skipped) > 20:
                self.stderr.write(self.style.WARNING(f"... and {len(skipped) - 20} more skipped lines"))
            self.stderr.write(
                self.style.WARNING(f"Skipped {len(skipped)} row(s) with errors (use --strict to abort instead).")
            )

        # Hosted Postgres (e.g. Supabase) often sets a low statement_timeout; DELETE of
        # millions of rows can exceed it. TRUNCATE is fast; extend timeout for bulk INSERT.
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = 0")
                if options["clear"]:
                    cursor.execute('TRUNCATE TABLE "transcripts" CASCADE')
                    self.stdout.write(self.style.WARNING("Cleared table transcripts."))
                cursor.executemany(INSERT_SQL, batch)

        self.stdout.write(
            self.style.SUCCESS(
                f"Inserted {len(batch)} row(s) from {path.name}"
                + (f" ({len(skipped)} skipped)." if skipped else ".")
            )
        )
