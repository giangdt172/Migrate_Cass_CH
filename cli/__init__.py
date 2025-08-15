import click
from cli.export_blocks_job import export_blocks_to_clickhouse
from cli.export_transactions_job import export_transactions_to_clickhouse
from cli.export_transfer_job import export_transfer_to_clickhouse
from cli.export_internal_transactions_job import export_internal_transactions_to_clickhouse
from cli.export_transaction_receipts_job import export_transaction_receipts_to_clickhouse

@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass

cli.add_command(export_blocks_to_clickhouse, "export_blocks_to_clickhouse")
cli.add_command(export_transactions_to_clickhouse, "export_transactions_to_clickhouse")
cli.add_command(export_transfer_to_clickhouse, "export_transfer_to_clickhouse")
cli.add_command(export_internal_transactions_to_clickhouse, "export_internal_transactions_to_clickhouse")
cli.add_command(export_transaction_receipts_to_clickhouse, "export_transaction_receipts_to_clickhouse")