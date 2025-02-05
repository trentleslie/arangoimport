"""Command line interface for arangoimport."""

import os
from typing import Any

import click
from rich.console import Console

from .importer import ImportConfig, parallel_load_data
from .logging import get_logger, setup_logging

console = Console()
logger = get_logger(__name__)


@click.group()
@click.version_option()
def cli() -> None:
    """ArangoImport - Import data into ArangoDB with ease."""
    setup_logging()


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--host", default="localhost", help="ArangoDB host")
@click.option("--port", type=int, default=8529, help="ArangoDB port")
@click.option(
    "--username", envvar="ARANGO_USER", default="root", help="Database username"
)
@click.option(
    "--password", envvar="ARANGO_PASSWORD", required=True, help="Database password"
)
@click.option("--db-name", default="spoke_human", help="Database name")
@click.option(
    "--processes", type=int, help="Number of processes to use (default: CPU count - 1)"
)
def import_data(file_path: str, **kwargs: Any) -> None:
    """Import data from a file into ArangoDB.

    Args:
        file_path: Path to input file
        **kwargs: Additional configuration options
    """
    if not kwargs.get("password"):
        raise ValueError("Password is required")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        # Create database configuration
        db_config = {
            "host": kwargs.get("host", "localhost"),
            "port": kwargs.get("port", 8529),
            "username": kwargs.get("username", "root"),
            "password": kwargs["password"],
            "db_name": kwargs.get("db_name", "spoke_human"),
        }

        # Create import configuration with default settings
        import_config = ImportConfig()

        nodes_added, edges_added = parallel_load_data(
            file_path,
            db_config,
            processes=kwargs.get("processes", 4),
            import_config=import_config
        )

        console.print("[green]Import successfully completed![/green]")
        console.print(f"Total nodes added: {nodes_added:,}")
        console.print(f"Total edges added: {edges_added:,}")

    except ConnectionError as e:
        console.print(f"[red]Unable to establish connection: {e!s}[/red]")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error: {e!s}[/red]")
        raise click.Abort() from e


def main() -> None:
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
