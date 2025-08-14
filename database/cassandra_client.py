import logging
import sys
import time
import os

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra import DriverException
from cassandra.concurrent import execute_concurrent_with_args
from utils.parse_cassandra_connection_elements import parse_cassandra_connection_elements
from configs.config import CassandraConfig
from utils.db_utils import round_timestamp, round_number

logger = logging.getLogger('Cassandra Client')

class CassandraClient:
    def __init__(self, connection_url=None, keyspace=None, block_partitions = 10000, tx_partitions = 100):
        if not connection_url:
            connection_url = CassandraConfig.CONNECTION_URL
        
        self.block_partitions = block_partitions
        self.tx_partitions = tx_partitions
        try:
            host, port, username, password = parse_cassandra_connection_elements(connection_url)
            self.connection_url = f'{host}:{port}'
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            self._cluster = Cluster([host], port=port, auth_provider=auth_provider, connect_timeout=10)
            self._session = self._cluster.connect()
            logger.info(f'Successfully connected to Cassandra')                
        except Exception as err:
            logger.warning(f'Failed to connect Cassandra: {connection_url}')
            logger.exception(err)
            sys.exit(1)
        self.keyspace = keyspace

    def open(self):
        pass

    def close(self):
        pass
        
    def get_blocks_data(self, numbers):
        if not numbers:
            return
        bucket_ids = [str(round_timestamp(i, self.block_partitions)) for i in numbers]
        bucket_ids = set(bucket_ids)
        string_buck_id = ','.join(bucket_ids)
        string_numbers = ','.join(str(i) for i in numbers)
        query = f"""
                    SELECT * FROM {self.keyspace}.blocks
                    WHERE  bucket_id IN ({string_buck_id})
                    AND number IN ({string_numbers})
                    ALLOW FILTERING;
                """
        response = self._session.execute(query).all()
        blocks = [block._asdict() for block in response]
        return blocks
    
    def get_transactions_data(self, numbers):
        if not numbers:
            return
        bucket_ids = [str(round_timestamp(i, self.tx_partitions)) for i in numbers]
        bucket_ids = set(bucket_ids)
        string_buck_id = ','.join(bucket_ids)
        string_numbers = ','.join(str(i) for i in numbers)
        query = f"""
                SELECT * FROM {self.keyspace}.transactions
                WHERE bucket_id IN ({string_buck_id})
                AND block_number IN ({string_numbers});
            """
        response = self._session.execute(query).all()
        transactions = [transaction._asdict() for transaction in response]
        return transactions
    
    def get_logs_data(self, start_block, end_block):
        start_buck_id = round_timestamp(start_block, self.tx_partitions)
        end_buck_id = round_timestamp(end_block, self.tx_partitions)
        if start_buck_id < end_buck_id:
            string_buck_id = ','.join(set(str(i) for i in range(start_buck_id, end_buck_id + self.tx_partitions, self.tx_partitions)))
            query = f"""
                    SELECT * FROM {self.keyspace}.logs
                    WHERE bucket_id IN ({string_buck_id})
                    AND block_number >= {start_block} AND block_number <= {end_block}
                """
        else:
            query = f"""
                    SELECT * FROM {self.keyspace}.logs
                    WHERE bucket_id = {start_buck_id}
                    AND block_number >= {start_block} AND block_number <= {end_block}
                """
        response = self._session.execute(query).all()
        logs = [log._asdict() for log in response]
        return logs

    def get_token_transfers_data(self, numbers):
        if not numbers:
            return
        bucket_ids = [str(round_timestamp(i, self.tx_partitions)) for i in numbers]
        bucket_ids = set(bucket_ids)
        string_buck_id = ','.join(bucket_ids)
        string_numbers = ','.join(str(i) for i in numbers)
        query = f"""
                SELECT * FROM {self.keyspace}.token_transfer
                WHERE bucket_id IN ({string_buck_id})
                AND block_number IN ({string_numbers});
            """
        response = self._session.execute(query).all()
        token_transfers = [token_transfer._asdict() for token_transfer in response]
        return token_transfers

    def get_internal_transactions_data(self, numbers):
        if not numbers:
            return
        bucket_ids = [str(round_timestamp(i, self.tx_partitions)) for i in numbers]
        bucket_ids = set(bucket_ids)
        string_buck_id = ','.join(bucket_ids)
        string_numbers = ','.join(str(i) for i in numbers)
        query = f"""
                SELECT * FROM {self.keyspace}.internal_transactions
                WHERE bucket_id IN ({string_buck_id})
                AND block_number IN ({string_numbers});
            """
        response = self._session.execute(query).all()
        internal_transactions = [internal_transaction._asdict() for internal_transaction in response]
        return internal_transactions