import logging
import os
import time

from utils.logging_utils import logging_basic_config, write_monitor_logs
from utils.file_utils import smart_open

class Streamer:
    """
    This class is for running StreamerAdapter and resolver errors.
    It will run StreamerAdapter:
        * from block last_synced_block -> last_synced_block + block_batch_size (or start_block)
        * or from start_block -> end_block
    Properties:
        blockchain_streamer_adapter: the StreamerAdapter that needs running
        start_block: optional, the first block to crawl
        end_block: optional, the las block to crawl
        periods_seconds: time sleep
        last_synced_block_file: for last_synced_block be used as start_block instead
        block_batch_size: for (start_block + block_batch_sized) to be used as end_block
        stream_id: id of the collector a.k.a. the collector's type (saved on exporter database)
    """
    def __init__(
        self,
        blockchain_streamer_adapter=None,
        last_synced_block_file='last_synced_block.txt',
        lag=0,
        start_block=None,
        end_block=None,
        period_seconds=10,
        block_batch_size=10,
        retry_errors=True,
        pid_file=None,
        stream_id=None,
        exporter=None,
        chain_id=None,
        monitor=False
    ):
        self.monitor = monitor
        self.chain_id = chain_id
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_file = last_synced_block_file
        self.lag = lag
        self.start_block = start_block
        self.end_block = end_block
        self.period_seconds = period_seconds
        self.block_batch_size = block_batch_size
        self.retry_errors = retry_errors
        self.pid_file = pid_file
        self.stream_id = stream_id
        self.exporter = exporter

        if self.start_block is not None or not os.path.isfile(self.last_synced_block_file):
            init_last_synced_block_file((self.start_block or 0) - 1, self.last_synced_block_file)

        self.last_synced_block = read_last_synced_block(self.last_synced_block_file)

    #     self.update_start_extract_at()

    # def update_start_extract_at(self):
    #     """Get the last block number from MongoDb"""
    #     if self.exporter:
    #         collector = self.exporter.get_collector(self.stream_id)
    #         if not collector.get("start_extracting_at_block_number"):
    #             collector = {
    #                 "id": self.stream_id,
    #                 "start_extracting_at_block_number": self.last_synced_block
    #             }
    #             self.exporter.update_collector(collector)

    def stream(self):
        try:
            if self.pid_file is not None:
                logging.info('Creating pid file {}'.format(self.pid_file))
                write_to_file(self.pid_file, str(os.getpid()))
            self.blockchain_streamer_adapter.open()
            self._do_stream()
        finally:
            self.blockchain_streamer_adapter.close()
            if self.pid_file is not None:
                logging.info('Deleting pid file {}'.format(self.pid_file))
                delete_file(self.pid_file)

    def _do_stream(self):
        """the name is kind of self-explanatory. Basically just continuously try-catch exception
        for the StreamerAdapter"""
        while self.end_block is None or self.last_synced_block < self.end_block:
            synced_blocks = 0
            # synced_blocks = self._sync_cycle()
            try:
                synced_blocks = self._sync_cycle()
            except Exception as e:
                # https://stackoverflow.com/a/4992124/1580227
                logging.exception('An exception occurred while syncing block data.')
                if not self.retry_errors:
                    raise e

            if synced_blocks <= 0:
                logging.info('Nothing to sync. Sleeping for {} seconds...'.format(self.period_seconds))
                time.sleep(self.period_seconds)

    def _sync_cycle(self):
        """Make the StreamerAdapter process from last_synced_block+1 to target_block"""
        current_block = self.blockchain_streamer_adapter.get_current_block_number()  # current block of collector
        target_block = self._calculate_target_block(current_block, self.last_synced_block)  # target block of worker
        blocks_to_sync = max(target_block - self.last_synced_block, 0)
        logging.info('Current block {}, target block {}, last synced block {}, blocks to sync {}'.format(
            current_block, target_block, self.last_synced_block, blocks_to_sync))

        if blocks_to_sync != 0:
            self.blockchain_streamer_adapter.export_all(self.last_synced_block + 1, target_block)
            logging.info('Writing last synced block {}'.format(target_block))
            write_last_synced_block(self.last_synced_block_file, target_block)
            if self.monitor:
                write_monitor_logs(f"{self.chain_id}_{self.stream_id}", target_block, self.chain_id)
            # if self.exporter:
            #     self.exporter.update_latest_updated_at(self.stream_id, target_block)
            self.last_synced_block = target_block

        return blocks_to_sync

    def _calculate_target_block(self, current_block, last_synced_block):
        """target_block: next block for the collector
        = min(last_synced_block + self.block_batch_size, current_block - self.lag, self.end_block)
        """
        if self.end_block is not None:
            return min(current_block - self.lag, last_synced_block + self.block_batch_size, self.end_block)
        else:
            return min(current_block - self.lag, last_synced_block + self.block_batch_size)


def delete_file(file):
    try:
        os.remove(file)
    except OSError:
        pass


def write_last_synced_block(file, last_synced_block):
    write_to_file(file, str(last_synced_block) + '\n')


def init_last_synced_block_file(start_block, last_synced_block_file):
    if os.path.isfile(last_synced_block_file):
        raise ValueError(
            '{} should not exist if --start-block option is specified. '
            'Either remove the {} file or the --start-block option.'
            .format(last_synced_block_file, last_synced_block_file))
    write_last_synced_block(last_synced_block_file, start_block)


def read_last_synced_block(file):
    with smart_open(file, 'r') as last_synced_block_file:
        return int(last_synced_block_file.read())


def write_to_file(file, content):
    with smart_open(file, 'w') as file_handle:
        file_handle.write(content)


