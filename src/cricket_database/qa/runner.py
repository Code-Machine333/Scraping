"""Data Quality Check Runner

Runs SQL data quality checks and presents results in a summary table.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Tuple

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import create_engine, text

from ..config import settings

console = Console()


def run_quality_checks() -> List[Tuple[str, int, str]]:
    """Run all data quality checks and return results."""
    engine = create_engine(settings.database.url)
    results = []
    
    # Get the project root directory
    project_root = Path(__file__).parent.parent.parent.parent
    qa_dir = project_root / "qa"
    
    # Run data quality checks
    dq_file = qa_dir / "data_quality_checks.sql"
    if dq_file.exists():
        with engine.connect() as conn:
            with open(dq_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Split by semicolon and execute each query
            queries = [q.strip() for q in sql_content.split(';') if q.strip()]
            
            for query in queries:
                if query.upper().startswith('SELECT'):
                    try:
                        result = conn.execute(text(query))
                        for row in result:
                            if len(row) >= 3:
                                results.append((row[0], row[1], row[2]))
                    except Exception as e:
                        console.print(f"[red]Error running query: {e}[/red]")
                        console.print(f"[yellow]Query: {query[:100]}...[/yellow]")
    
    # Run duplicate detection checks
    dup_file = qa_dir / "duplicate_detection.sql"
    if dup_file.exists():
        with engine.connect() as conn:
            with open(dup_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Split by semicolon and execute each query
            queries = [q.strip() for q in sql_content.split(';') if q.strip()]
            
            for query in queries:
                if query.upper().startswith('SELECT'):
                    try:
                        result = conn.execute(text(query))
                        for row in result:
                            if len(row) >= 3:
                                results.append((row[0], row[1], row[2]))
                    except Exception as e:
                        console.print(f"[red]Error running query: {e}[/red]")
                        console.print(f"[yellow]Query: {query[:100]}...[/yellow]")
    
    return results


def display_results(results: List[Tuple[str, int, str]]) -> None:
    """Display quality check results in a formatted table."""
    if not results:
        console.print("[yellow]No quality check results found.[/yellow]")
        return
    
    table = Table(title="Data Quality Check Results")
    table.add_column("Check Name", style="cyan", no_wrap=True)
    table.add_column("Issue Count", style="red", justify="right")
    table.add_column("Description", style="white")
    
    total_issues = 0
    critical_issues = 0
    
    for check_name, issue_count, description in results:
        total_issues += issue_count
        
        # Mark critical issues (nulls in FKs, duplicate matches, etc.)
        if any(keyword in check_name.lower() for keyword in ['missing', 'duplicate', 'mismatch']):
            critical_issues += issue_count
            style = "red"
        elif issue_count > 0:
            style = "yellow"
        else:
            style = "green"
        
        table.add_row(
            check_name,
            str(issue_count),
            description,
            style=style
        )
    
    console.print(table)
    
    # Summary
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"Total issues found: [red]{total_issues}[/red]")
    console.print(f"Critical issues: [red]{critical_issues}[/red]")
    
    if critical_issues == 0:
        console.print("[green]✅ No critical data quality issues found![/green]")
    else:
        console.print(f"[red]⚠️ {critical_issues} critical issues require attention[/red]")


def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed output"),
    output_file: str = typer.Option(None, "--output", "-o", help="Save results to file")
):
    """Run data quality checks and display results."""
    console.print("[bold]Running Data Quality Checks...[/bold]")
    
    try:
        results = run_quality_checks()
        display_results(results)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("Data Quality Check Results\n")
                f.write("=" * 50 + "\n\n")
                for check_name, issue_count, description in results:
                    f.write(f"{check_name}: {issue_count} - {description}\n")
            console.print(f"[green]Results saved to {output_file}[/green]")
            
    except Exception as e:
        console.print(f"[red]Error running quality checks: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    typer.run(main)
