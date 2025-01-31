"""Command line interface for arangoimport."""

import click
from rich.console import Console

from .connection import ArangoConfig
from .importer import parallel_load_data
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
@click.option("--db-name", default="spoke_human", help="Database name")
@click.option(
    "--username", envvar="ARANGO_USER", default="root", help="Database username"
)
@click.option(
    "--password", envvar="ARANGO_PASSWORD", required=True, help="Database password"
)
@click.option(
    "--processes", type=int, help="Number of processes to use (default: CPU count - 1)"
)
@click.option("--host", default="localhost", help="ArangoDB host")
@click.option("--port", type=int, default=8529, help="ArangoDB port")
def import_data(
    file_path: str,
    **kwargs: str | int,
) -> None:
    """Import data from a file into ArangoDB.

    Args:
        file_path: Path to the input file
        **kwargs: Additional configuration options including:
            - db_name: Target database name
            - username: Database username
            - password: Database password
            - processes: Number of processes to use
            - host: ArangoDB host
            - port: ArangoDB port
    """
    try:
        # Extract and type-cast processes
        processes = kwargs.pop("processes", None)
        num_processes: int | None = int(processes) if processes is not None else None

        # Create database configuration
        db_config = ArangoConfig(
            host=str(kwargs.get("host", "localhost")),
            port=int(kwargs.get("port", 8529)),
            username=str(kwargs.get("username", "root")),
            password=str(kwargs["password"]),
            db_name=str(kwargs.get("db_name", "spoke_human")),
        )

        nodes_added, edges_added = parallel_load_data(
            file_path, dict(db_config), num_processes=num_processes
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
