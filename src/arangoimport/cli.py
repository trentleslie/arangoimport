"""Command line interface for arangoimport."""

import os
from typing import Any
import json

import click
from rich.console import Console

from .importer import ImportConfig, parallel_load_data
from .id_mapping import IDMapper
from .log_config import get_logger, setup_logging
from arango.client import ArangoClient
from arango.database import Database
from arango.exceptions import ArangoClientError, ArangoServerError, ArangoError

# Import provider functionality
from .providers import list_providers

console = Console()
logger = get_logger(__name__)


@click.group()
@click.version_option()
@click.option(
    "--log-level",
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False),
    default='WARNING',
    help='Set the logging level (default: WARNING)'
)
def cli(log_level: str) -> None:
    """ArangoImport - Import data into ArangoDB with ease."""
    setup_logging(level_str=log_level)


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--host", default="localhost", help="ArangoDB host and optionally port (e.g., localhost or localhost:8529)")
@click.option("--port", type=int, default=8529, help="ArangoDB port (ignored if port is in host)")
@click.option("--db-name", default="arango_import_db", help="Database name")
# Provider option will be added in future updates
# @click.option(
#     "--provider",
#     default="spoke",
#     type=click.Choice(list_providers(), case_sensitive=False),
#     help="Data provider to use for import (default: spoke)"
# )
@click.option(
    "--username", envvar="ARANGO_USER", default="root", help="Database username"
)
@click.option(
    "--password", envvar="ARANGO_PASSWORD", default="", help="Database password"
)
@click.option("--processes", type=int, default=os.cpu_count(), help="Number of worker processes")
@click.option("--create-db", is_flag=True, default=True, help="Create database if it doesn't exist (default: True)")
@click.option("--overwrite-db", is_flag=True, default=False, help="Drop database if it exists before import (default: False)")
@click.option("--collection-nodes", default="Nodes", help="Node collection name")
@click.option("--collection-edges", default="Edges", help="Edge collection name")
@click.option("--batch-size", type=int, default=1000, help="Batch size for imports")
@click.option("--stop-on-error", is_flag=True, default=False, help="Stop import on first error")
@click.pass_context
def import_data(
    ctx: click.Context,
    file_path: str,
    host: str,
    port: int,
    db_name: str,
    username: str,
    password: str,
    processes: int,
    create_db: bool,
    overwrite_db: bool,
    collection_nodes: str,
    collection_edges: str,
    batch_size: int,
    stop_on_error: bool,
) -> None:
    """Import data from a JSONL file into ArangoDB."""
    # Retrieve log_level from the parent context
    log_level = ctx.parent.params['log_level']

    # Parse host and port
    if ":" in host:
        host_part, port_part_str = host.split(":", 1)
        try:
            port_part = int(port_part_str)
            host = host_part
            port = port_part  # Override port option if specified in host
        except ValueError:
            console.print(f"[red]Error: Invalid port '{port_part_str}' in host string '{host}'.[/red]")
            raise click.Abort()

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        # Create database configuration
        db_config = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "db_name": db_name,
            "create_db": create_db,
            "overwrite_db": overwrite_db,
            "collection_nodes": collection_nodes,
            "collection_edges": collection_edges,
            "batch_size": batch_size,
            "stop_on_error": stop_on_error,
        }
        
        # Provider configuration will be added in future updates
        # Temporarily commented out for compatibility
        # provider_config = {
        #     "provider": provider,
        #     "file_path": file_path,
        # }

        # Create import configuration with settings
        import_config = ImportConfig(
            skip_missing_refs=True
        )

        logger.info(
            f"Starting import for {file_path} into {host}:{port}/{db_name} "
            f"using {processes} processes."
        )
        _nodes_added, _edges_added = parallel_load_data(
            file_path,
            db_config=db_config,
            processes=processes,
            import_config=import_config,
            log_level_str=log_level
        )
        
        # Get quality metrics
        metrics = import_config.get_metrics()
        metrics_dict = metrics.to_dict()
        
        console.print("[green]Import successfully completed![/green]")
        console.print("\n[blue]Quality Metrics:[/blue]")
        console.print(f"Total documents processed: {metrics_dict['total_documents']:,}")
        console.print(f"Valid documents: {metrics_dict['valid_documents']:,}")
        console.print(f"Invalid documents: {metrics_dict['invalid_documents']:,}")
        console.print(f"Duplicates found: {metrics_dict['duplicates_found']:,}")
        console.print(f"Missing references: {metrics_dict['missing_references']:,}")
        console.print(f"Validity ratio: {metrics_dict['validity_ratio']:.2%}")
        
        if metrics_dict['validation_errors']:
            console.print("\n[yellow]Validation Errors (first 5):[/yellow]")
            for error in metrics_dict['validation_errors'][:5]:
                console.print(f"- {error}")

    except ConnectionError as e:
        console.print(f"[red]Unable to establish connection: {e!s}[/red]")
        raise click.Abort() from e
    except (ArangoClientError, ArangoServerError, ArangoError) as e:
        error_msg = getattr(e, 'error_message', str(e))
        console.print(f"[red]ArangoDB error: {error_msg}[/red]")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error: {e!s}[/red]")
        raise click.Abort() from e


@cli.command()
@click.option("--host", default="localhost", help="ArangoDB host")
@click.option("--port", type=int, default=8529, help="ArangoDB port")
@click.option(
    "--username", envvar="ARANGO_USER", default="root", help="Database username"
)
@click.option(
    "--password", envvar="ARANGO_PASSWORD", default="", help="Database password"
)
@click.option("--db-name", default="spokeV6", help="Database name")
@click.option("--query", "query_string", required=True, help="AQL query to execute")
def query_db(
    host: str,
    port: int,
    username: str,
    password: str,
    db_name: str,
    query_string: str,
) -> None:
    """Execute an AQL query against the specified ArangoDB database."""
    try:
        # Connect using python-arango
        conn_url = f"http://{host}:{port}"
        client = ArangoClient(hosts=conn_url)
        # System database connection needed to access specific DB
        sys_db = client.db("_system", username=username, password=password)
        # Check if target database exists and access it
        if not sys_db.has_database(db_name):
            console.print(f"[red]Database '{db_name}' not found.[/red]")
            raise click.Abort()
        db = client.db(db_name, username=username, password=password)

        logger.info(f"Executing query on database '{db_name}': {query_string}")
        # Execute the query using python-arango API
        cursor = db.aql.execute(query_string)

        # Print results as JSON list
        results_list = list(cursor)
        console.print(json.dumps(results_list, indent=2))

    except (ArangoClientError, ArangoServerError) as e:
        console.print(f"[red]ArangoDB query error: {e.http_exception.response.status_code} {e.http_exception.response.reason} - {e.error_message}[/red]")
        logger.error(f"AQL query failed: {e}")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error during query: {e!s}[/red]")
        logger.error(f"Unexpected query error: {e}")
        raise click.Abort() from e


@cli.command()
@click.argument("db_name_to_drop")
@click.option("--host", default="localhost", help="ArangoDB host")
@click.option("--port", type=int, default=8529, help="ArangoDB port")
@click.option(
    "--username", envvar="ARANGO_USER", default="root", help="Database username"
)
@click.option(
    "--password", envvar="ARANGO_PASSWORD", default="", help="Database password"
)
@click.option("--yes", is_flag=True, help="Confirm database deletion without prompting.")
def drop_db(
    db_name_to_drop: str,
    host: str,
    port: int,
    username: str,
    password: str,
    yes: bool,
) -> None:
    """Drop (delete) the specified ArangoDB database."""
    if not yes:
        click.confirm(
            f"Are you sure you want to drop the database '{db_name_to_drop}'?",
            abort=True,
        )

    try:
        # Connect using python-arango
        conn_url = f"http://{host}:{port}"
        client = ArangoClient(hosts=conn_url)
        # System database connection needed to drop other databases
        sys_db = client.db("_system", username=username, password=password)

        # Check if target database exists
        if not sys_db.has_database(db_name_to_drop):
            console.print(f"[yellow]Database '{db_name_to_drop}' not found. Nothing to drop.[/yellow]")
            return # Exit gracefully

        logger.info(f"Attempting to drop database '{db_name_to_drop}'")
        # Drop the database
        if sys_db.delete_database(db_name_to_drop):
            console.print(f"[green]Database '{db_name_to_drop}' dropped successfully.[/green]")
            logger.info(f"Database '{db_name_to_drop}' dropped.")
        else:
            # This case might not be reachable if has_database is accurate
            # and permissions are correct, but good to handle.
            console.print(f"[red]Failed to drop database '{db_name_to_drop}'. It might not exist or there could be a permission issue.[/red]")
            logger.warning(f"Failed to drop database '{db_name_to_drop}'.")
            raise click.Abort()

    except (ArangoClientError, ArangoServerError) as e:
        # Check if it's a 'database not found' error, which is okay in this context
        if hasattr(e, 'http_exception') and hasattr(e.http_exception, 'response') and e.http_exception.response.status_code == 404 and e.error_code == 1228: # 1228 is ERROR_ARANGO_DATABASE_NOT_FOUND
             console.print(f"[yellow]Database '{db_name_to_drop}' not found. Nothing to drop.[/yellow]")
        else:
            error_msg = f"{e.http_exception.response.status_code} {e.http_exception.response.reason} - {e.error_message}"
            console.print(f"[red]ArangoDB error during drop: {error_msg}[/red]")
            logger.error(f"Database drop failed: {e}")
            raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error during database drop: {e!s}[/red]")
        logger.error(f"Unexpected drop error: {e}")
        raise click.Abort() from e


def main() -> None:
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
