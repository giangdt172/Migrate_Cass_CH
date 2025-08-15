from jobs.base_job import BaseJob
from executors.batch_work_executor import BatchWorkExecutor
import logging

_LOGGER = logging.getLogger(__name__)


class ExportTransactionReceipts(BaseJob):
    def __init__(self, start_block, end_block, item_importer, item_exporter, batch_size, max_workers):
        self.start_block = start_block
        self.end_block = end_block
        self.item_importer = item_importer
        self.item_exporter = item_exporter
        self.batch_work_executor = BatchWorkExecutor(
            starting_batch_size=batch_size, 
            max_workers=max_workers
        )
    def _start(self):
        self.item_importer.open()

    def _export(self):
        _LOGGER.info(f"Exporting transactions receipts from {self.start_block} to {self.end_block}")
        total_blocks = self.end_block - self.start_block + 1
        
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1), 
            self.read_and_export_transaction_receipts_batch,
            total_items=total_blocks
        )

    def read_and_export_transaction_receipts_batch(self, block_numbers):
        transaction_receipts = self.item_importer.get_transaction_receipts_data(block_numbers)
        self.item_exporter.upsert_transaction_receipts(transaction_receipts)

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()