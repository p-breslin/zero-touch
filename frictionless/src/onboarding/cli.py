import asyncio
import json
import logging
import sys

import click

from configs import cfg
from src.onboarding.auth import authenticate
from src.onboarding.customer import (
    create_customer,
    delete_customer,
    generate_customer_token,
    poll_customer_db,
)
from src.onboarding.file_uploader import upload_and_wait
from src.onboarding.metrics import (
    compute_metrics,
    fetch_compute_job_status,
    wait_for_compute_completion,
)
from src.onboarding.package import set_package, set_product
from src.onboarding.poller import wait_for
from src.onboarding.publish import publish_metrics
from utils.async_helpers import file_upload_wrapper
from utils.helpers import confirm_with_timeout
from utils.logger import setup_logging
from utils.model_validation import validate_model

setup_logging(stream=False)
log = logging.getLogger(__name__)


# Asynchronous functions (for the file upload)
async def async_upload_data(client, infos, cfg):
    tasks = [
        file_upload_wrapper(
            client,
            info,
            cfg.FILE_UPLOAD_PATH,
            cfg.POLLING_INTERVAL_SECONDS,
            cfg.TIMEOUT_SECONDS,
        )
        for info in infos
    ]
    return await asyncio.gather(*tasks)


@click.group(invoke_without_command=True)
@click.pass_context  # shares the setup for every command
def cli(ctx):
    """Onboarding toolkit for xFlow."""
    client = authenticate(cfg)
    click.echo("Client authenticated")
    ctx.obj = {"cfg": cfg, "client": client}

    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command()
@click.pass_context
def setup_customer(ctx):
    """Sets a customer up within the system."""
    cfg = ctx.obj["cfg"]
    client = ctx.obj["client"]

    # Customer & token creation
    click.echo(f"Creating customer for {cfg.NEW_CUSTOMER_PAYLOAD['email']}")
    resp = create_customer(client, cfg.NEW_CUSTOMER_PAYLOAD)

    click.echo(f"Company Name: {resp['Company']}")
    click.echo(f"Industry: {resp['Industry']}")
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])

    # Set tier
    set_product(client, cfg.SET_PRODUCT_PAYLOAD)
    set_package(client, cfg.SET_PACKAGE_PAYLOAD)

    click.echo("Setting up customer database...")
    wait_for(
        poll_customer_db,
        client,
        interval=cfg.POLLING_INTERVAL_SECONDS,
        timeout=cfg.TIMEOUT_SECONDS / 2,
        on_retry=lambda _: click.echo("Database not ready", err=True),
        on_timeout=lambda e: click.echo(
            f"DB polling timed out after {e:.1f}s", err=True
        ),
    )
    click.echo("Customer database ready")


@cli.command()
@click.pass_context
def model_validation(ctx):
    """Validates the model selected for the newly created customer."""
    cfg = ctx.obj["cfg"]
    client = ctx.obj["client"]
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])
    click.echo("Validating the model...")
    validate_model(client, cfg.NEW_CUSTOMER_PAYLOAD["industryId"])


@cli.command()
@click.pass_context
def sync_upload_data(ctx):
    """Synchronously uploads data for the created customer."""
    cfg = ctx.obj["cfg"]
    client = ctx.obj["client"]
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])

    click.echo("Uploading data files...")
    for info in (cfg.DEMO_DATA_INFO, cfg.KPI_DATA_INFO):
        upload_and_wait(
            client,
            file_info=info,
            base_path=cfg.FILE_UPLOAD_PATH,
            interval=cfg.POLLING_INTERVAL_SECONDS,
            timeout=cfg.TIMEOUT_SECONDS,
        )
    click.echo("All files uploaded")


@cli.command()
@click.pass_context
def upload_data(ctx):
    """Asynchronously uploads data for the created customer."""
    cfg = ctx.obj["cfg"]
    client = ctx.obj["client"]
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])
    click.echo("Uploading data files...")
    asyncio.run(async_upload_data(client, (cfg.DEMO_DATA_INFO, cfg.KPI_DATA_INFO), cfg))
    click.echo("All data files uploaded")


@cli.command()
@click.pass_context
def metric_compute(ctx):
    """Compute metrics for the customer."""
    cfg = ctx.obj["cfg"]
    client = ctx.obj["client"]
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])

    # Kick off the compute and get the external job ID
    click.echo("Computing metrics...")
    job_id = compute_metrics(client)

    # Poll until the compute job completes
    wait_for_compute_completion(
        client,
        job_id,
        interval=cfg.POLLING_INTERVAL_SECONDS,
        timeout=cfg.TIMEOUT_SECONDS * 2,
    )
    click.echo("Metrics computed")

    # Fetch and display final job status
    status = fetch_compute_job_status(client, job_id)
    click.echo(json.dumps(status, indent=2))

    # Option to publish metrics (with timeout)
    if confirm_with_timeout("Publish the computed metrics?", timeout=15, default=True):
        publish_metrics(client, job_id)
        click.echo("Metrics published")
    else:
        click.echo("You can publish metrics later using `cli.py publish --job-id`")


@cli.command()
@click.pass_context
@click.option("--job-id", required=True, help="Job ID for the metric computation.")
def publish(ctx, job_id):
    """Publish the metric computation for the given job ID."""
    client = ctx.obj["client"]
    click.echo("Publishing metrics...")
    publish_metrics(client, job_id)
    click.echo("Metrics published")


@cli.command()
@click.pass_context
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
def cleanup(ctx, yes):
    """Delete the onboarded customer (direct MySQL cleanup)."""
    cfg = ctx.obj["cfg"]
    client = ctx.obj["client"]

    if yes or click.confirm(f"Delete customer {cfg.NEW_CUSTOMER_PAYLOAD['email']}?"):
        success = delete_customer(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])
        if not success:
            sys.exit(1)
        click.echo("Customer deleted.")
    else:
        click.echo("Cleanup aborted.")


@cli.command()
@click.pass_context
def run_onboarding(ctx):
    """Runs the full onboarding pipeline end-to-end."""
    ctx.invoke(setup_customer)
    ctx.invoke(model_validation)
    ctx.invoke(upload_data)
    ctx.invoke(metric_compute)

    # Optional deletion with timeout-based confirmation
    email = ctx.obj["cfg"].NEW_CUSTOMER_PAYLOAD["email"]
    prompt = f"Delete customer {email}?"
    if confirm_with_timeout(prompt, timeout=15, default=False):
        ctx.invoke(cleanup, yes=True)
    else:
        click.echo("Skipped cleanup.")


if __name__ == "__main__":
    cli()
