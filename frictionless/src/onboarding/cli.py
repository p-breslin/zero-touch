import sys

import click

import config
from onboarding.auth import authenticate
from onboarding.customer import (
    create_customer,
    delete_customer,
    generate_customer_token,
)
from onboarding.file_uploader import upload_and_wait
from onboarding.metrics import (
    compute_metrics,
    fetch_compute_job_status,
    wait_for_compute_completion,
)
from onboarding.package import set_package, set_product
from onboarding.poller import PollResult, wait_for
from utils.logger import setup_logging
from utils.model_validation import validate_model

setup_logging()


@click.group()
def cli():
    """Onboarding toolkit for xFlow."""
    pass


@cli.command()
def onboard():
    """Runs the full onboarding pipeline end-to-end."""
    cfg = config
    client = authenticate(cfg)

    # 1) Customer creation & token
    create_customer(client, cfg.NEW_CUSTOMER_PAYLOAD)
    generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])

    # 2) Product & package
    set_product(client, cfg.SET_PRODUCT_PAYLOAD)
    set_package(client, cfg.SET_PACKAGE_PAYLOAD)

    # 3) Wait for DB ready
    def _db_predicate() -> PollResult:
        status = client.check_db_status()
        payload = status.get("payload", {})
        if payload.get("db_exists"):
            return PollResult(done=True, value=payload)
        click.echo("Database not ready; retrying...", err=True)
        return PollResult(done=False)

    wait_for(
        _db_predicate,
        interval=cfg.POLLING_INTERVAL_SECONDS,
        timeout=cfg.TIMEOUT_SECONDS / 2,
        on_timeout=lambda e: click.echo(
            f"DB polling timed out after {e:.1f}s", err=True
        ),
    )
    click.echo("Database is ready")

    # 4) Validate model
    validate_model(client, cfg.NEW_CUSTOMER_PAYLOAD["industryId"])

    # 5) File uploads
    for info in (cfg.DEMO_DATA_INFO, cfg.KPI_DATA_INFO):
        upload_and_wait(
            client,
            file_info=info,
            base_path=cfg.FILE_UPLOAD_PATH,
            interval=cfg.POLLING_INTERVAL_SECONDS,
            timeout=cfg.TIMEOUT_SECONDS,
        )

    click.echo("All files uploaded")

    # 6) Metrics compute
    job_id = compute_metrics(
        client, cfg.POLLING_INTERVAL_SECONDS, cfg.TIMEOUT_SECONDS * 2
    )
    wait_for_compute_completion(
        client,
        job_id,
        interval=cfg.POLLING_INTERVAL_SECONDS,
        timeout=cfg.TIMEOUT_SECONDS * 2,
    )
    click.echo("Metrics computed")

    # 7) Show final job status
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
