"""Main CLI interface for cricket database system."""

import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from ..etl import ETLPipeline
from ..etl.raw_fetch import cli_fetch as raw_cli_fetch
from ..etl.parse_load import run_parse_load
from ..etl.parse_scorecard import parse_scorecard
from ..etl.transform import to_rows
from ..etl.load import load_rows
from ..etl.config import get_etl_config
from ..database import get_database_engine
from ..etl.reconcile import reconcile_main
from ..database import create_tables, drop_tables, get_database_engine
from ..config import settings
from ..utils.migrate_sql import migrate as run_sql_migrations

# Initialize rich console
console = Console()

# Configure logging
def setup_logging(level: str = "INFO", log_file: Optional[str] = None):
    """Setup logging configuration."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Setup console handler with rich
    console_handler = RichHandler(console=console, show_time=True, show_path=False)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    
    # Setup file handler if specified
    handlers = [console_handler]
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        handlers=handlers,
        force=True
    )


app = typer.Typer(
    name="cricket-database",
    help="Cricket Database System - Production-grade cricket data ETL pipeline",
    no_args_is_help=True
)

# Global state
app_state = {"dry_run": False, "verbose": False}

@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
    log_file: Optional[str] = typer.Option(None, "--log-file", help="Log file path"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Run in dry-run mode (no database changes)")
):
    """Cricket Database System - Production-grade cricket data ETL pipeline."""
    # Setup logging
    log_level = "DEBUG" if verbose else settings.logging.level
    setup_logging(log_level, log_file)
    
    # Store state
    app_state['dry_run'] = dry_run
    app_state['verbose'] = verbose
    
    # Display banner
    if not verbose:
        console.print("\n[bold blue]🏏 Cricket Database System[/bold blue]")
        console.print("Production-grade cricket data ETL pipeline\n")


@app.command()
def setup_db(force: bool = typer.Option(False, "--force", help="Force recreation of tables")):
    """Initialize database schema."""
    console.print("[bold]Setting up database schema...[/bold]")
    
    try:
        if force:
            console.print("Dropping existing tables...")
            drop_tables()
        
        console.print("Creating database tables...")
        create_tables()
        
        console.print("[green]✅ Database schema initialized successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Database setup failed: {e}[/red]")
        raise typer.Exit(1)


@app.command("migrate-sql")
def migrate_sql(
    force_reapply: bool = typer.Option(False, "--force-reapply", help="Reapply even if previously applied")
):
    """Apply SQL migrations from db/ddl in lexical order."""
    console.print("[bold]Applying SQL migrations...[/bold]")
    try:
        engine = get_database_engine()
        results = run_sql_migrations(engine, force_reapply=force_reapply)
        table = Table(title="SQL Migrations Applied")
        table.add_column("File", style="cyan")
        table.add_column("Statements", style="green")
        table.add_column("Status", style="magenta")
        for filename, count, status in results:
            table.add_row(filename, str(count), status)
        console.print(table)
        console.print("[green]✅ SQL migrations completed[/green]")
    except Exception as e:
        console.print(f"[red]❌ SQL migrations failed: {e}[/red]")
        raise typer.Exit(1)


@app.command("refresh-season-all")
def refresh_season_all(season_id: int = typer.Argument(..., help="Season ID to refresh")):
    """Run stored procedure to refresh all season summaries and series leaders."""
    console.print(f"[bold]Refreshing season summaries for season_id={season_id}...[/bold]")
    try:
        engine = get_database_engine()
        with engine.begin() as conn:
            conn.exec_driver_sql("CALL refresh_season_all(%s)", (season_id,))
        console.print("[green]✅ Season summaries refreshed[/green]")
    except Exception as e:
        console.print(f"[red]❌ Refresh failed: {e}[/red]")
        raise typer.Exit(1)


@app.command("fetch")
def fetch(
    series_key: Optional[str] = typer.Option(None, "--series-key", help="Year or competition key"),
    from_date: Optional[str] = typer.Option(None, "--from-date", help="Start date YYYY-MM-DD"),
    max_pages: Optional[int] = typer.Option(None, "--max-pages", help="Limit pages"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Do not persist (informational only)"),
    use_browser: bool = typer.Option(False, "--browser", help="Use Playwright (Chromium)"),
    headers_only: bool = typer.Option(False, "--headers-only", help="HEAD requests only; do not download bodies"),
    max_new_matches: Optional[int] = typer.Option(None, "--max-new-matches", help="Safety cap on new matches/pages per run")
):
    """Polite raw fetcher for index/match lists/scorecards."""
    try:
        asyncio.run(raw_cli_fetch(series_key, from_date, max_pages, dry_run, use_browser, headers_only, max_new_matches))
        console.print("[green]✅ Fetch completed[/green]")
    except Exception as e:
        console.print(f"[red]❌ Fetch failed: {e}[/red]")
        raise typer.Exit(1)


@app.command("parse-load")
def parse_load(
    limit: int = typer.Option(10, "--limit", help="Number of raw_html rows to parse"),
    days_back: Optional[int] = typer.Option(None, "--days-back", help="Only parse rows fetched in the last N days"),
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Skip DB writes (default: dry-run)")
):
    """Parse recent raw_html scorecards and print a summary. DB upserts are off by default."""
    try:
        summaries = run_parse_load(limit=limit, days_back=days_back, dry_run=dry_run)
        table = Table(title="Parse Summaries")
        table.add_column("raw_id", style="cyan")
        table.add_column("match_key", style="magenta")
        table.add_column("teams", style="green")
        table.add_column("innings", style="yellow")
        table.add_column("warnings", style="red")
        for s in summaries:
            teams = ", ".join(s.get("teams", []))
            warns = str(len(s.get("warnings", [])))
            table.add_row(str(s.get("raw_id")), str(s.get("source_match_key")), teams, str(s.get("innings")), warns)
        console.print(table)
        if dry_run:
            console.print("[yellow]Dry-run: no DB writes performed[/yellow]")
        else:
            console.print("[green]✅ Parsed and loaded[/green]")
    except Exception as e:
        console.print(f"[red]❌ Parse-load failed: {e}[/red]")
        raise typer.Exit(1)


@app.command("load")
def load(
    match_url: Optional[str] = typer.Option(None, "--match-url", help="Absolute or relative URL to fetch and load"),
    from_raw: Optional[int] = typer.Option(None, "--from-raw", help="raw_html.id to parse and load"),
    use_browser: bool = typer.Option(False, "--browser", help="Use Playwright for JS pages"),
):
    """Parse and load a single match by URL or existing raw_html id (transaction per match)."""
    if not match_url and not from_raw:
        console.print("[red]❌ Provide --match-url or --from-raw[/red]")
        raise typer.Exit(2)
    cfg = get_etl_config()
    engine = get_database_engine()
    try:
        if match_url:
            # Reuse raw fetcher to get body
            from ..etl.raw_fetch import RawFetcher
            fetcher = RawFetcher(use_browser=use_browser)
            status, body, etag = awaitable_fetch(fetcher, match_url)
            html_text = body.decode("utf-8", errors="ignore")
        else:
            with engine.connect() as conn:
                row = conn.exec_driver_sql("SELECT url, body FROM raw_html WHERE id=%s", (from_raw,)).fetchone()
                if not row:
                    console.print("[red]❌ raw_html not found[/red]")
                    raise typer.Exit(3)
                match_url, html_text = str(row[0]), str(row[1])

        match, warnings = parse_scorecard(html_text, page_url=match_url)
        rows = to_rows(match, cfg.sources.cricketarchive_source_id)
        load_rows(engine, rows)
        console.print("[green]✅ Match loaded successfully[/green]")
        if warnings:
            console.print(f"[yellow]Warnings: {len(warnings)}[/yellow]")
    except Exception as e:
        console.print(f"[red]❌ Load failed: {e}[/red]")
        raise typer.Exit(1)


def awaitable_fetch(fetcher, url: str):
    import asyncio
    return asyncio.get_event_loop().run_until_complete(fetcher._fetch(url))


@app.command("reconcile")
def reconcile(
    report: Optional[str] = typer.Option(None, "--report", help="Comma-separated report keys e.g. missing_matches,dup_players,counts")
):
    """Run reconciliation reports against the existing Cricinfo DB (CRICINFO_RO_DSN)."""
    if not report:
        console.print("[yellow]No report specified; defaulting to 'counts'[/yellow]")
        report = "counts"
    keys = [k.strip() for k in report.split(",") if k.strip()]
    try:
        outputs = reconcile_main(keys)
        table = Table(title="Reconciliation Outputs")
        table.add_column("Report", style="cyan")
        table.add_column("Path", style="green")
        for k, v in outputs.items():
            table.add_row(k, v)
        console.print(table)
    except Exception as e:
        console.print(f"[red]❌ Reconciliation failed: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def scrape(
    source: str = typer.Option("all", "--source", help="Data source to scrape from", 
                              click_type=typer.Choice(['all', 'espn', 'cricket_api'])),
    data_type: str = typer.Option("all", "--data-type", help="Type of data to scrape",
                                 click_type=typer.Choice(['all', 'teams', 'players', 'matches'])),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit number of records to scrape")
):
    """Scrape cricket data from sources."""
    dry_run = app_state['dry_run']
    
    if dry_run:
        console.print("[yellow]🔍 Running in dry-run mode - no data will be saved[/yellow]")
    
    console.print(f"[bold]Scraping {data_type} data from {source}...[/bold]")
    
    async def run_scraping():
        pipeline = ETLPipeline(dry_run=dry_run)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Scraping data...", total=None)
            
            try:
                if data_type == 'all':
                    results = await pipeline.run_full_pipeline()
                else:
                    # Run specific data type scraping
                    results = await pipeline._extract_data()
                    if limit:
                        results[data_type] = results.get(data_type, [])[:limit]
                
                progress.update(task, description="✅ Scraping completed")
                
                # Display results
                display_scraping_results(results)
                
            except Exception as e:
                progress.update(task, description=f"❌ Scraping failed: {e}")
                console.print(f"[red]Scraping failed: {e}[/red]")
                raise typer.Exit(1)
    
    asyncio.run(run_scraping())


@app.command()
def update(
    incremental: bool = typer.Option(False, "--incremental", help="Run incremental update"),
    days_back: int = typer.Option(7, "--days-back", help="Days back for incremental update"),
    full: bool = typer.Option(False, "--full", help="Run full data refresh")
):
    """Update cricket database with latest data."""
    dry_run = app_state['dry_run']
    
    if dry_run:
        console.print("[yellow]🔍 Running in dry-run mode - no data will be saved[/yellow]")
    
    if not (incremental or full):
        console.print("[red]❌ Please specify either --incremental or --full[/red]")
        raise typer.Exit(1)
    
    console.print("[bold]Updating cricket database...[/bold]")
    
    async def run_update():
        pipeline = ETLPipeline(dry_run=dry_run)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Updating data...", total=None)
            
            try:
                if incremental:
                    results = await pipeline.run_incremental_update(days_back)
                else:
                    results = await pipeline.run_full_pipeline()
                
                progress.update(task, description="✅ Update completed")
                
                # Display results
                display_update_results(results)
                
            except Exception as e:
                progress.update(task, description=f"❌ Update failed: {e}")
                console.print(f"[red]Update failed: {e}[/red]")
                raise typer.Exit(1)
    
    asyncio.run(run_update())


@app.command()
def quality_check():
    """Run data quality checks."""
    console.print("[bold]Running data quality checks...[/bold]")
    
    async def run_quality_checks():
        pipeline = ETLPipeline()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Checking data quality...", total=None)
            
            try:
                results = await pipeline._run_quality_checks()
                progress.update(task, description="✅ Quality checks completed")
                
                # Display results
                display_quality_results(results)
                
            except Exception as e:
                progress.update(task, description=f"❌ Quality checks failed: {e}")
                console.print(f"[red]Quality checks failed: {e}[/red]")
                raise typer.Exit(1)
    
    asyncio.run(run_quality_checks())


@app.command()
def validate_sources():
    """Validate data sources connectivity."""
    console.print("[bold]Validating data sources...[/bold]")
    
    async def run_validation():
        pipeline = ETLPipeline()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Validating sources...", total=None)
            
            try:
                results = await pipeline.validate_data_sources()
                progress.update(task, description="✅ Validation completed")
                
                # Display results
                display_validation_results(results)
                
            except Exception as e:
                progress.update(task, description=f"❌ Validation failed: {e}")
                console.print(f"[red]Validation failed: {e}[/red]")
                raise typer.Exit(1)
    
    asyncio.run(run_validation())


@app.command()
def status():
    """Show system status and configuration."""
    console.print("[bold]System Status[/bold]")
    
    async def show_status():
        pipeline = ETLPipeline()
        status_info = await pipeline.get_pipeline_status()
        
        # Display configuration
        config_table = Table(title="Pipeline Configuration")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="green")
        
        for key, value in status_info["pipeline_config"].items():
            config_table.add_row(key, str(value))
        
        console.print(config_table)
        
        # Display scraper configuration
        scraper_table = Table(title="Scraper Configuration")
        scraper_table.add_column("Setting", style="cyan")
        scraper_table.add_column("Value", style="green")
        
        for key, value in status_info["scraper_config"].items():
            scraper_table.add_row(key, str(value))
        
        console.print(scraper_table)
        
        # Display data quality configuration
        quality_table = Table(title="Data Quality Configuration")
        quality_table.add_column("Setting", style="cyan")
        quality_table.add_column("Value", style="green")
        
        for key, value in status_info["data_quality_config"].items():
            quality_table.add_row(key, str(value))
        
        console.print(quality_table)
    
    asyncio.run(show_status())


@app.command()
def schedule(
    schedule: str = typer.Option("0 2 * * *", "--schedule", help="Cron schedule (default: daily at 2 AM)"),
    incremental: bool = typer.Option(False, "--incremental", help="Run incremental updates"),
    days_back: int = typer.Option(7, "--days-back", help="Days back for incremental updates")
):
    """Schedule automated data updates."""
    console.print(f"[bold]Scheduling automated updates with cron: {schedule}[/bold]")
    
    try:
        import schedule
        import time
        
        def run_update():
            """Run the update process."""
            console.print(f"[blue]🕐 Running scheduled update at {datetime.now()}[/blue]")
            
            async def update_task():
                pipeline = ETLPipeline()
                if incremental:
                    results = await pipeline.run_incremental_update(days_back)
                else:
                    results = await pipeline.run_full_pipeline()
                
                if results["status"] == "success":
                    console.print("[green]✅ Scheduled update completed successfully[/green]")
                else:
                    console.print(f"[red]❌ Scheduled update failed: {results.get('error', 'Unknown error')}[/red]")
            
            asyncio.run(update_task())
        
        # Schedule the job
        if schedule == '0 2 * * *':  # Daily at 2 AM
            schedule.every().day.at("02:00").do(run_update)
        elif schedule == '0 */6 * * *':  # Every 6 hours
            schedule.every(6).hours.do(run_update)
        elif schedule == '0 */1 * * *':  # Every hour
            schedule.every().hour.do(run_update)
        else:
            console.print("[red]❌ Unsupported schedule format. Use standard cron format.[/red]")
            raise typer.Exit(1)
        
        console.print("[green]✅ Scheduler started. Press Ctrl+C to stop.[/green]")
        
        # Keep the scheduler running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        console.print("\n[yellow]⏹️ Scheduler stopped by user[/yellow]")
    except ImportError:
        console.print("[red]❌ Schedule library not installed. Install with: pip install schedule[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]❌ Scheduler failed: {e}[/red]")
        raise typer.Exit(1)


def display_scraping_results(results):
    """Display scraping results in a formatted table."""
    if isinstance(results, dict) and "extraction" in results:
        # Full pipeline results
        table = Table(title="Scraping Results")
        table.add_column("Data Type", style="cyan")
        table.add_column("Records", style="green")
        
        for data_type, count in results["extraction"].items():
            table.add_row(data_type.title(), str(count))
        
        console.print(table)
        
        # Show duration
        if "duration_seconds" in results:
            console.print(f"[blue]⏱️ Total duration: {results['duration_seconds']:.2f} seconds[/blue]")
    else:
        # Raw extraction results
        table = Table(title="Scraping Results")
        table.add_column("Data Type", style="cyan")
        table.add_column("Records", style="green")
        
        for data_type, records in results.items():
            table.add_row(data_type.title(), str(len(records)))
        
        console.print(table)


def display_update_results(results):
    """Display update results in a formatted table."""
    if results["status"] == "success":
        console.print("[green]✅ Update completed successfully![/green]")
        
        # Show extraction results
        if "extraction" in results:
            table = Table(title="Data Extracted")
            table.add_column("Data Type", style="cyan")
            table.add_column("Records", style="green")
            
            for data_type, count in results["extraction"].items():
                table.add_row(data_type.title(), str(count))
            
            console.print(table)
        
        # Show loading results
        if "loading" in results and not results["loading"].get("dry_run"):
            table = Table(title="Data Loaded")
            table.add_column("Data Type", style="cyan")
            table.add_column("Inserted", style="green")
            table.add_column("Updated", style="yellow")
            table.add_column("Errors", style="red")
            
            for data_type, stats in results["loading"].items():
                if isinstance(stats, dict):
                    table.add_row(
                        data_type.title(),
                        str(stats.get("inserted", 0)),
                        str(stats.get("updated", 0)),
                        str(stats.get("errors", 0))
                    )
            
            console.print(table)
        
        # Show duration
        if "duration_seconds" in results:
            console.print(f"[blue]⏱️ Total duration: {results['duration_seconds']:.2f} seconds[/blue]")
    else:
        console.print(f"[red]❌ Update failed: {results.get('error', 'Unknown error')}[/red]")


def display_quality_results(results):
    """Display quality check results."""
    if results.get("status") == "disabled":
        console.print("[yellow]⚠️ Data quality checks are disabled[/yellow]")
        return
    
    console.print(f"[bold]Overall Quality Score: {results.get('overall_score', 0)}/100[/bold]")
    
    if "checks" in results:
        table = Table(title="Quality Check Results")
        table.add_column("Data Type", style="cyan")
        table.add_column("Total Records", style="blue")
        table.add_column("Issues", style="red")
        table.add_column("Quality Score", style="green")
        
        for check_name, check_result in results["checks"].items():
            if isinstance(check_result, dict):
                total_records = check_result.get("total_teams", 
                                               check_result.get("total_players",
                                               check_result.get("total_matches",
                                               check_result.get("total_innings",
                                               check_result.get("total_balls",
                                               check_result.get("total_stats", 0))))))
                issues_count = len(check_result.get("issues", []))
                quality_score = check_result.get("quality_score", 0)
                
                table.add_row(
                    check_name.title(),
                    str(total_records),
                    str(issues_count),
                    f"{quality_score}/100"
                )
        
        console.print(table)


def display_validation_results(results):
    """Display validation results."""
    table = Table(title="Data Source Validation")
    table.add_column("Source", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Details", style="blue")
    
    for source, result in results.items():
        status = result.get("status", "unknown")
        details = ""
        
        if status == "success":
            status_style = "green"
            if "teams_found" in result:
                details = f"Found {result['teams_found']} teams"
        elif status == "failed":
            status_style = "red"
            details = result.get("error", "Unknown error")
        else:
            status_style = "yellow"
            details = "Unknown status"
        
        table.add_row(source.title(), f"[{status_style}]{status}[/{status_style}]", details)
    
    console.print(table)


if __name__ == '__main__':
    app()
