from jobs.export_blocks import ExportBlocks

class ExportBlocksAdapter:
    def __init__(
            self,
            batch_size,
            max_workers,
            collector_id,
            item_importer,
            item_exporter,
    ):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.collector_id = collector_id
        self.item_importer = item_importer
        self.item_exporter = item_exporter

    def open(self):
        self.item_importer.open()

    def close(self):
        self.item_importer.close()

    def get_current_block_number(self):
        return 2e32

    def export_all(self, start_block, end_block):
        self._export_blocks(start_block, end_block)

    def _export_blocks(self, start_block, end_block):
        job = ExportBlocks(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_importer=self.item_importer,
            item_exporter=self.item_exporter,
        )
        job.run()
