from __future__ import annotations

import asyncio
import json


import typer
from rich.console import Console
from rich.table import Table

from cricket_database.etl.config import get_etl_config
from cricket_database.etl.raw_fetch import RawFetcher
from cricket_database.etl.parse_scorecard import parse_scorecard
from cricket_database.etl.transform import to_rows
from cricket_database.etl.load import load_rows
from cricket_database.database import get_database_engine
from .metrics_server import serve as serve_metrics


app = typer.Typer(name="etl", no_args_is_help=True, help="Incremental ETL CLI")
console = Console()


QUEUE_DIR = Path("data/queue")
CACHE_DIR = Path("data/cache/parsed")
METRICS_FILE = Path("data/cache/metrics_last.json")
QUEUE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _enqueue(item: Dict):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    path = QUEUE_DIR / f"{ts}.json"
    path.write_text(json.dumps(item), encoding="utf-8")


def _read_queue() -> List[Path]:
    return sorted(QUEUE_DIR.glob("*.json"))


def _dequeue(path: Path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass


@app.command("discover-latest")
def discover_latest(since: Optional[str] = typer.Option(None, "--since", help="YYYY-MM-DD")):
    """Discover recent series/competitions and enqueue match list pages."""
    cfg = get_etl_config()
    base = str(cfg.scraper.cricketarchive_base_url)
    if since:
        year = since.split("-")[0]
    else:
        year = str(datetime.now(timezone.utc).year)
    # Enqueue the series index for the target year
    url = f"{base}/Archive/Events/{year}.html"
    _enqueue({"type": "series_index", "url": url})
    console.print(f"[green]Enqueued series index[/green] {url}")


@app.command("fetch")
def fetch(max_items: int = typer.Option(50, "--max-items"), use_browser: bool = typer.Option(False, "--browser"), headers_only: bool = typer.Option(False, "--headers-only"), dry_run: bool = typer.Option(False, "--dry-run")):
    """Download raw pages for queued items and store to raw_html (idempotent)."""
    cfg = get_etl_config()
    fetcher = RawFetcher(use_browser=use_browser, dry_run=dry_run, headers_only=headers_only)
    items = _read_queue()[:max_items]
    if not items:
        console.print("[yellow]Queue empty[/yellow]")
        return

    async def run():
        for p in items:
            payload = json.loads(p.read_text(encoding="utf-8"))
            url = payload.get("url")
            if not url:
                _dequeue(p)
                continue
            status, body, etag = await fetcher._fetch(url)
            # Persist handled by RawFetcher helpers in separate flows; here we only fetch & let parse/load use DB
            # Keep item for parse stage; dequeue now to avoid re-fetch loops
            _dequeue(p)
            console.print(f"[green]Fetched[/green] {url} status={status}")

    asyncio.run(run())


@app.command("queue")
def queue(action: str = typer.Argument("list", help="list|prune"), keep: int = typer.Option(200, "--keep", help="Keep newest N when pruning")):
    """Inspect or prune the on-disk ETL queue."""
    items = _read_queue()
    if action == "list":
   
        for p in items[:200]:
            try:
                payload = json.loads(p.read_text(encoding="utf-8"))
                table.add_row(p.name, payload.get("url", ""))
            except Exception:
                table.add_row(p.name, "<invalid>")
        console.print(table)
        console.print(f"[blue]Total queued:[/blue] {len(items)}")
    elif action == "prune":
        remove = items[:-keep] if len(items) > keep else []
        for p in remove:
            _dequeue(p)
        console.print(f"[green]Pruned[/green] {len(remove)} old items; kept {min(len(items), keep)}")
    else:
        console.print("[red]Unknown action; use list or prune[/red]")


@app.command("metrics")
def metrics(json_out: bool = typer.Option(False, "--json", help="Output metrics as JSON")):
    """Show basic ETL metrics (queue depth, cached models, recent raw count)."""
    q = len(_read_queue())
    cached = len(list(CACHE_DIR.glob("*.json")))
    engine = get_database_engine()
    with engine.connect() as conn:
        raw_24h = conn.exec_driver_sql("SELECT COUNT(*) FROM raw_html WHERE fetched_at >= NOW() - INTERVAL 1 DAY").scalar()
    data = {
        "queue_depth": q,
        "cached_models": cached,
        "raw_html_24h": int(raw_24h or 0),
        "last_refresh": None,
        "durations": None,
    }
    if METRICS_FILE.exists():
        try:
            m = json.loads(METRICS_FILE.read_text(encoding="utf-8"))
            data["last_refresh"] = m.get("timestamp")
            data["durations"] = m.get("durations")
        except Exception:
            pass
    if json_out:
        console.print_json(data=data)
        return
    table = Table(title="ETL Metrics")
    table.add_column("metric", style="cyan")
    table.add_column("value", style="green")
    table.add_row("queue_depth", str(data["queue_depth"]))
    table.add_row("cached_models", str(data["cached_models"]))
    table.add_row("raw_html_24h", str(data["raw_html_24h"]))
    if data["last_refresh"]:
        table.add_row("last_refresh", data["last_refresh"]) 
    console.print(table)
    if data.get("durations"):
        dtab = Table(title="Last Refresh Durations (s)")
        dtab.add_column("step", style="cyan")
        dtab.add_column("seconds", style="green")
        for k, v in data["durations"].items():
            dtab.add_row(k, f"{v:.3f}")
        console.print(dtab)


@app.command("parse")
def parse(max_items: int = typer.Option(50, "--max-items")):
    """Parse latest raw_html rows into cached JSON models under data/cache/parsed."""
    engine = get_database_engine()
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(
            "SELECT id, url, body FROM raw_html ORDER BY fetched_at DESC LIMIT %s", (max_items,)
        ).fetchall()
    count = 0
    for rid, url, body in rows:
        try:
            match, warnings = parse_scorecard(str(body), page_url=str(url))
            key = match.source_match_key or f"raw{rid}"
            out_path = CACHE_DIR / f"{key}.json"
            out_path.write_text(match.model_dump_json(indent=2), encoding="utf-8")
            count += 1
        except Exception as e:
            console.print(f"[red]Parse failed raw_id={rid}[/red] {e}")
    console.print(f"[green]Parsed and cached[/green] {count} models")


@app.command("load")
def load(max_items: int = typer.Option(20, "--max-items")):
    """Load cached parsed models into DB idempotently (transaction per match)."""
    cfg = get_etl_config()
    engine = get_database_engine()
    files = sorted(CACHE_DIR.glob("*.json"))[:max_items]
    if not files:
        console.print("[yellow]No cached models found[/yellow]")
        return
    loaded = 0
    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            # Reconstruct MatchModel via Pydantic (lazy import to avoid cycles)
            from cricket_database.etl.models import MatchModel
            m = MatchModel.model_validate(data)
            rows = to_rows(m, cfg.sources.cricketarchive_source_id)
            load_rows(engine, rows)
            loaded += 1
        except Exception as e:
            console.print(f"[red]Load failed for {f.name}[/red] {e}")
    console.print(f"[green]Loaded[/green] {loaded} matches")


@app.command("refresh")
def refresh(since: Optional[str] = typer.Option(None, "--since", help="YYYY-MM-DD")):
    """Run incremental refresh: discover-latest -> fetch -> parse -> load."""
    t0 = datetime.now(timezone.utc)
    def _timed(step, fn):
        s = datetime.now(timezone.utc)
        fn()
        e = (datetime.now(timezone.utc) - s).total_seconds()
        return step, e
    durations = {}
    step, sec = _timed("discover", lambda: discover_latest(since=since))
    durations[step] = sec
    step, sec = _timed("fetch", fetch)
    durations[step] = sec
    step, sec = _timed("parse", parse)
    durations[step] = sec
    step, sec = _timed("load", load)
    durations[step] = sec
    METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
    METRICS_FILE.write_text(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "durations": durations,
    }, indent=2), encoding="utf-8")
    console.print("[green]Incremental refresh completed[/green]")
    # Show durations table
    dtab = Table(title="Refresh Durations (s)")
    dtab.add_column("step", style="cyan")
    dtab.add_column("seconds", style="green")
    for k, v in durations.items():
        dtab.add_row(k, f"{v:.3f}")
    console.print(dtab)


if __name__ == "__main__":
    app()


@app.command("serve-metrics")
def serve_metrics_cmd(host: str = typer.Option("127.0.0.1", "--host"), port: int = typer.Option(9109, "--port")):
    """Serve Prometheus metrics for the last refresh durations at /metrics."""
    serve_metrics(host=host, port=port)


