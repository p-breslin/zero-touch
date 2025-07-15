import asyncio
import logging
import sys

import click

import config
from src.onboarding.auth import authenticate
from src.onboarding.customer import (
    create_customer,
    delete_customer,
    generate_customer_token,
)
from src.onboarding.file_uploader import upload_and_wait
from src.onboarding.metrics import (
    compute_metrics,
    fetch_compute_job_status,
    wait_for_compute_completion,
)
from src.onboarding.package import set_package, set_product
from src.onboarding.poller import PollResult, wait_for
from utils.async_helpers import file_upload_wrapper
from utils.logger import setup_logging
from utils.model_validation import validate_model

setup_logging()
log = logging.getLogger(__name__)


# Asynchronous functions
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


@click.group()
def cli():
    """Onboarding toolkit for xFlow."""
    pass


@cli.command()
def setup_customer():
    """Sets a customer up within the system."""
    cfg = config
    client = authenticate(cfg)

    # Customer creation & token
    create_customer(client, cfg.NEW_CUSTOMER_PAYLOAD)
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])

    # Product & package
    set_product(client, cfg.SET_PRODUCT_PAYLOAD)
    set_package(client, cfg.SET_PACKAGE_PAYLOAD)

    # Wait for DB setup
    def _db_poll() -> PollResult:
        status = client.check_db_status()
        payload = status.get("payload", {})
        if payload.get("db_exists"):
            return PollResult(done=True, value=payload)
        click.echo("Database not ready; retrying...", err=True)
        return PollResult(done=False)

    wait_for(
        _db_poll,
        interval=cfg.POLLING_INTERVAL_SECONDS,
        timeout=cfg.TIMEOUT_SECONDS / 2,
        on_timeout=lambda e: click.echo(
            f"DB polling timed out after {e:.1f}s", err=True
        ),
    )
    log.info("Database is ready.")


@cli.command()
def model_validation():
    """Validates the model selected for the newly created customer."""
    cfg = config
    client = authenticate(cfg)
    validate_model(client, cfg.NEW_CUSTOMER_PAYLOAD["industryId"])


@cli.command()
def sync_upload_data():
    """Synchronously uploads data for the created customer."""
    cfg = config
    client = authenticate(cfg)
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])
    for info in (cfg.DEMO_DATA_INFO, cfg.KPI_DATA_INFO):
        upload_and_wait(
            client,
            file_info=info,
            base_path=cfg.FILE_UPLOAD_PATH,
            interval=cfg.POLLING_INTERVAL_SECONDS,
            timeout=cfg.TIMEOUT_SECONDS,
        )
    log.info("All files uploaded.")


@cli.command()
def upload_data():
    """Asynchronously uploads data for the created customer."""
    cfg = config
    client = authenticate(cfg)
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])
    asyncio.run(async_upload_data(client, (cfg.DEMO_DATA_INFO, cfg.KPI_DATA_INFO), cfg))
    log.info("All files uploaded.")


@cli.command()
def metric_compute():
    """Computes metrics for the customer."""
    cfg = config
    client = authenticate(cfg)

    job_id = compute_metrics(client, cfg)
    wait_for_compute_completion(
        client,
        job_id,
        interval=cfg.POLLING_INTERVAL_SECONDS,
        timeout=cfg.TIMEOUT_SECONDS * 2,
    )
    log.info("Metrics computed.")

    # Show final job status
    status = fetch_compute_job_status(client)
    click.echo(status)


@cli.command()
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
def cleanup(yes):
    """Delete the onboarded customer (direct MySQL cleanup)."""
    cfg = config
    client = authenticate(cfg)

    if yes or click.confirm(f"Delete customer {cfg.NEW_CUSTOMER_PAYLOAD['email']}?"):
        success = delete_customer(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])
        if not success:
            sys.exit(1)
    else:
        click.echo("Cleanup aborted.")


if __name__ == "__main__":
    cli()
