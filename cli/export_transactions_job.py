import click

from utils.logging_utils import logging_basic_config
from database.cassandra_client import CassandraClient
from database.clickhouse_client import ClickhouseClient
from streaming.export_transactions_adapter import ExportTransactionsAdapter
from streaming.streamer import Streamer

@click.command()
@click.option('-d', '--db-prefix', default='', help='Database prefix')
@click.option('-s', '--start-block', type=int, default=1, help='Starting block number')
@click.option('-e', '--end-block', type=int, required=True, help='Ending block number')
@click.option('-b', '--batch-size', type=int, default=1000, help='Batch size')
@click.option('-w', '--max-workers', type=int, default=10, help='Maximum number of workers')
@click.option('-c', '--chain-id', type=int, default=1, help='Chain ID')
def export_transactions_to_clickhouse(db_prefix, start_block, end_block, batch_size, max_workers, chain_id):
    logging_basic_config()

    item_importer = CassandraClient(connection_url='',keyspace='blockchain_etl')
    item_exporter = ClickhouseClient(connection_url='clickhouse+native://default:123456789@localhost:9000/default', db_prefix=db_prefix)

    adapter = ExportTransactionsAdapter(
        batch_size=batch_size,
        max_workers=max_workers,
        collector_id=None,
        item_importer=item_importer,
        item_exporter=item_exporter
        )

    streamer = Streamer(
        blockchain_streamer_adapter=adapter,
        last_synced_block_file='last_synced_block.txt',
        lag=0,
        start_block=start_block,
        end_block=end_block,
        period_seconds=10,
        block_batch_size=batch_size,
        stream_id='transactions',
        exporter=item_exporter,
        chain_id=chain_id
    )

    streamer.stream()