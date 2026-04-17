"""Apply transcripts schema from Prisma migration (one-time / local setup)."""
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

BACKEND = Path(__file__).resolve().parent.parent
PROJECT = BACKEND.parent


def load_env(path: Path, *, override: bool = False) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip('"').strip("'")
        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = val


def _psql_database_url(url: str) -> str:
    """psql rejects some pooler query params (e.g. pgbouncer=true). Strip those."""
    p = urlparse(url)
    if not p.query:
        return url
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() != "pgbouncer"]
    new_query = urlencode(q)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


def main() -> int:
    load_env(PROJECT / "database" / ".env", override=True)
    load_env(BACKEND / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL not set (checked database/.env and backend/.env).", file=sys.stderr)
        return 1
    url = _psql_database_url(url)
    sql = (
        PROJECT
        / "database"
        / "prisma"
        / "migrations"
        / "20260416153242_init_search"
        / "migration.sql"
    )
    if not sql.is_file():
        print(f"Migration SQL not found: {sql}", file=sys.stderr)
        return 1
    psql = os.environ.get("PSQL_EXE", r"C:\Program Files\PostgreSQL\18\bin\psql.exe")
    r = subprocess.run(
        [psql, url, "-v", "ON_ERROR_STOP=1", "-f", str(sql)],
        capture_output=True,
        text=True,
    )
    if r.stdout:
        print(r.stdout)
    if r.stderr:
        print(r.stderr, file=sys.stderr)
    return r.returncode


if __name__ == "__main__":
    raise SystemExit(main())
