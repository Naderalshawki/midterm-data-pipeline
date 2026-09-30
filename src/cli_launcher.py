"""
Big Data Hybrid ELT & Analytics Command Center (Interactive CLI Launcher)
=========================================================================
Covers Phase 1 (Midterm 18 Grades) + Phase 2 (Final 7 Grades = 25/25)
"""
import os
import sys
import json
import subprocess
from pathlib import Path
from pymongo import MongoClient

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.columns import Columns
from rich.prompt import Prompt
from rich.text import Text
from rich import box

from config import settings
from src.main import execute_pipeline
from src.queries_and_indexes import (
    create_indexes_and_benchmark_explain,
    list_available_queries,
    execute_named_query,
)
from src.aggregations import list_available_aggregations, run_aggregation_report
from src.materialized_views import refresh_all_materialized_views
from src.scheduler import list_scheduled_jobs, run_job_by_name

console = Console()


BANNER_ASCII = r"""
 ██████╗ ██╗ ██████╗     ██████╗  █████╗ ████████╗ █████╗     ███████╗██╗  ████████╗
 ██╔══██╗██║██╔════╝     ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗    ██╔════╝██║  ╚══██╔══╝
 ██████╔╝██║██║  ███╗    ██║  ██║███████║   ██║   ███████║    █████╗  ██║     ██║   
 ██╔══██╗██║██║   ██║    ██║  ██║██╔══██║   ██║   ██╔══██║    ██╔══╝  ██║     ██║   
 ██████╔╝██║╚██████╔╝    ██████╔╝██║  ██║   ██║   ██║  ██║    ███████╗███████╗██║   
 ╚═════╝ ╚═╝ ╚═════╝     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝    ╚══════╝╚══════╝╚═╝   
          HYBRID PYSPARK & PYTHON STREAMING + MONGODB ANALYTICS ENGINE v2.0
"""


def get_live_db_stats():
    try:
        client = MongoClient(settings.MONGO_URI, serverSelectionTimeoutMS=1500)
        db = client[settings.MONGO_DATABASE]
        stats = {
            "raw": db[settings.RAW_COLLECTION].estimated_document_count(),
            "val": db[settings.VALIDATED_COLLECTION].estimated_document_count(),
            "quar": db[settings.QUARANTINE_COLLECTION].estimated_document_count(),
            "mv_daily": db[settings.MV_DAILY_SALES].estimated_document_count(),
            "mv_prod": db[settings.MV_TOP_PRODUCTS].estimated_document_count(),
            "jobs": db[settings.JOBS_LOG_COLLECTION].estimated_document_count(),
            "online": True,
        }
        client.close()
        return stats
    except Exception:
        return {"raw": 0, "val": 0, "quar": 0, "mv_daily": 0, "mv_prod": 0, "jobs": 0, "online": False}


def render_header():
    console.clear()
    console.print(Panel(Text(BANNER_ASCII, style="bold cyan", justify="center"),
                        title="[bold yellow]BIG DATA PIPELINE COMMAND CENTER (PHASE 1 + PHASE 2)[/bold yellow]",
                        subtitle="[bold green]Engineered by: Eng. Nader Alshawki[/bold green]",
                        border_style="bright_blue", box=box.DOUBLE))

    stats = get_live_db_stats()
    status_str = "[bold green]● ONLINE[/bold green]" if stats["online"] else "[bold red]● OFFLINE[/bold red]"

    p1 = Panel(
        f"[cyan]Master:[/cyan] local[*]\n[cyan]Threshold:[/cyan] {settings.SMALL_FILE_THRESHOLD_MB} MB\n[cyan]Batch Size:[/cyan] {settings.BATCH_SIZE:,}",
        title="[bold cyan]PySpark / Python Core[/bold cyan]",
        border_style="cyan", width=36
    )
    p2 = Panel(
        f"[magenta]DB Name:[/magenta] {settings.MONGO_DATABASE}\n[magenta]Status:[/magenta]  {status_str}\n[magenta]Validated:[/magenta] {stats['val']:,} docs",
        title="[bold magenta]MongoDB Engine[/bold magenta]",
        border_style="magenta", width=38
    )
    p3 = Panel(
        f"[green]Raw Docs:[/green] {stats['raw']:,} | [red]Quarantine:[/red] {stats['quar']:,}\n"
        f"[yellow]MVs (Daily/Prod):[/yellow] {stats['mv_daily']} / {stats['mv_prod']}\n"
        f"[blue]Scheduled Jobs Logged:[/blue] {stats['jobs']}",
        title="[bold yellow]Live Telemetry[/bold yellow]",
        border_style="yellow", width=44
    )
    console.print(Columns([p1, p2, p3]))


def render_menu():
    table = Table(title="[bold green]MAIN OPERATIONS & ANALYTICS MENU[/bold green]",
                  box=box.ROUNDED, border_style="bright_green", show_lines=False, expand=True)
    table.add_column("Key", style="bold yellow", justify="center", width=6)
    table.add_column("Phase", style="bold cyan", width=10)
    table.add_column("Operation", style="bold white", width=34)
    table.add_column("Description", style="dim white")

    table.add_row("[1]", "Phase 1", "Run Sample/Custom Pipeline", "Execute Hybrid ELT Pipeline on sample or custom CSV file")
    table.add_row("[2]", "Phase 1", "Run 30M Huge Dataset Pipeline", "Launch distributed PySpark ingestion + ELT on huge dataset")
    table.add_row("[3]", "Phase 2", "Build 3 Indexes & Run Explain", "Create Compound Indexes & benchmark explain('executionStats') Before/After")
    table.add_row("[4]", "Phase 2", "Execute 5 Operational Queries", "Run all 5 indexed MongoDB operational queries & display results")
    table.add_row("[5]", "Phase 2", "Run 5 Aggregation Reports", "Generate Sales by City, Top Products, Top Customers, Period & Status")
    table.add_row("[6]", "Phase 2", "Refresh Materialized Views", "Incremental refresh of daily_sales_summary & top_products_summary")
    table.add_row("[7]", "Phase 2", "Run Scheduled Jobs Manually", "Trigger both scheduled jobs immediately & log audit execution")
    table.add_row("[8]", "Phase 2", "Launch Unified FastAPI Server", "Start FastAPI server + Web Dashboard (/) + Swagger UI (/docs)")
    table.add_row("[9]", "Testing", "Run Full PyTest Verification", "Execute automated test suite (Phase 1 + Phase 2 API tests)")
    table.add_row("[10]", "System", "Check Collection Live Stats", "Inspect all MongoDB collections, watermarks, and counts")
    table.add_row("[0]", "Exit", "Exit Command Center", "Close application safely")

    console.print(table)


def main_loop():
    while True:
        render_header()
        render_menu()
        choice = Prompt.ask("\n[bold yellow]Select action [1-10 / 0 to Exit][/bold yellow]", default="1")

        if choice == "0":
            console.print("\n[bold green]Goodbye Eng. Nader! Best of luck in the evaluation![/bold green]")
            break

        elif choice == "1":
            custom_path = Prompt.ask("Enter CSV file path (Press Enter for default sample)", default=str(settings.SAMPLE_FILE))
            execute_pipeline(custom_path)

        elif choice == "2":
            huge_path = str(settings.DATA_DIR / "orders_huge_mixed_quality.csv")
            execute_pipeline(huge_path)

        elif choice == "3":
            console.print("\n[bold cyan][*] Building 3 Indexes & Benchmarking explain('executionStats')...[/bold cyan]")
            report = create_indexes_and_benchmark_explain()
            t = Table(title="Explain (executionStats) Before vs. After Indexes", box=box.DOUBLE_EDGE, border_style="cyan")
            t.add_column("Query Name", style="bold yellow")
            t.add_column("Before Stage", style="red")
            t.add_column("Before Docs", justify="right")
            t.add_column("Before Time", justify="right")
            t.add_column("After Stage", style="bold green")
            t.add_column("After Docs", justify="right")
            t.add_column("After Time", justify="right")
            for comp in report["explain_comparisons"]:
                b, a = comp["before_index"], comp["after_index"]
                t.add_row(
                    comp["query_name"],
                    b["scan_stage"], f"{b['totalDocsExamined']:,}", f"{b['executionTimeMillis']} ms",
                    a["scan_stage"], f"{a['totalDocsExamined']:,}", f"{a['executionTimeMillis']} ms"
                )
            console.print(t)

        elif choice == "4":
            for q in list_available_queries():
                res = execute_named_query(q["name"], limit=3)
                console.print(Panel(json.dumps(res["results"], ensure_ascii=False, indent=2),
                                    title=f"[bold yellow]{res['title']} (Returned: {res['count_returned']})[/bold yellow]",
                                    border_style="blue"))

        elif choice == "5":
            for agg in list_available_aggregations():
                res = run_aggregation_report(agg["name"], limit=5)
                console.print(Panel(json.dumps(res["results"], ensure_ascii=False, indent=2),
                                    title=f"[bold magenta]{res['title']}[/bold magenta]",
                                    border_style="magenta"))

        elif choice == "6":
            res = refresh_all_materialized_views(force_full=False)
            console.print(Panel(json.dumps(res, ensure_ascii=False, indent=2),
                                title="[bold green]Incremental Materialized Views Refresh Result[/bold green]",
                                border_style="green"))

        elif choice == "7":
            r1 = run_job_by_name("refresh_materialized_views_job")
            r2 = run_job_by_name("generate_periodic_report_job")
            console.print(Panel(json.dumps([r1, r2], ensure_ascii=False, indent=2),
                                title="[bold cyan]Scheduled Jobs Manual Execution Audit Log[/bold cyan]",
                                border_style="cyan"))

        elif choice == "8":
            console.print("\n[bold green]Starting FastAPI Server on http://localhost:8000 ...[/bold green]")
            console.print("[bold yellow] -> Web Dashboard : http://localhost:8000/[/bold yellow]")
            console.print("[bold yellow] -> Swagger Docs  : http://localhost:8000/docs[/bold yellow]")
            console.print("[dim]Press Ctrl+C to stop the server and return to menu.[/dim]\n")
            subprocess.run([sys.executable, "-m", "src.api"])

        elif choice == "9":
            console.print("\n[bold yellow]Running Automated PyTest Suite...[/bold yellow]")
            subprocess.run([sys.executable, "-m", "pytest", "tests/", "-v", "-s"])

        elif choice == "10":
            jobs_info = list_scheduled_jobs()
            console.print(Panel(json.dumps(jobs_info, ensure_ascii=False, indent=2),
                                title="[bold yellow]System & Scheduled Jobs Status[/bold yellow]", border_style="yellow"))

        Prompt.ask("\n[bold cyan]Press Enter to return to Command Center Menu[/bold cyan]")


if __name__ == "__main__":
    main_loop()