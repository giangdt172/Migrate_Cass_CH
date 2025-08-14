import collections
import logging
import threading
import time
import sys

from sqlalchemy import create_engine, text
from configs.config import ClickhouseConfig
from clickhouse_driver import connect

logger = logging.getLogger("Clickhouse Client")

class ClickhouseClient:
    def __init__(self, connection_url=None, db_prefix=''):
        self.connection_url = connection_url
        if db_prefix:
            self.database = f'{db_prefix}_{ClickhouseConfig.DATABASE}'
        else:
            self.database = ClickhouseConfig.DATABASE

        try:
            tmp_conn = connect(self.connection_url)
            self._local = threading.local()
            with tmp_conn.cursor() as cursor:
                self._tmp_cursor = cursor
                self.init_schema()
            tmp_conn.close()
        except Exception as err:
            logger.warning(f'Failed to connect Clickhouse: {self.connection_url}')
            logger.exception(err)
            sys.exit(1)
        
    def _get_connection(self):
        conn = getattr(self._local, 'connection', None)
        if conn is None:
            try:
                conn = connect(self.connection_url)
                self._local.connection = conn
            except Exception as e:
                logger.warning('Failed to create ClickHouse connection')
                logger.exception(e)
                raise
        return conn

    def _reconnect_current_thread(self):
        try:
            conn = getattr(self._local, 'connection', None)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            self._local.connection = connect(self.connection_url)
        except Exception as e:
            logger.warning('Failed to reconnect ClickHouse connection')
            logger.exception(e)
            raise
        
    def open(self):
        pass

    def close(self):
        conn = getattr(self._local, 'connection', None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    def execute_query(self, query: str, params: dict = None):
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as err:
            # Try one reconnect in case connection is closed
            msg = str(err).lower()
            if 'closed' in msg or 'disconnect' in msg:
                try:
                    self._reconnect_current_thread()
                    conn = self._get_connection()
                    with conn.cursor() as cursor:
                        cursor.execute(query, params)
                        return cursor.fetchall()
                except Exception:
                    pass
            logger.warning(f'Failed to execute query: {query}')
            logger.exception(err)
            return None

    def init_schema(self):
        logger.info(f"Creating database: {self.database}")
        self.execute_query(f"CREATE DATABASE IF NOT EXISTS {self.database}")

        logger.info("Creating blocks table...")
        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.blocks (
                number Int32,
                hash String,
                difficulty Nullable(String),
                extra_data Nullable(String),
                gas_limit Nullable(String),
                gas_used Nullable(String),
                logs_bloom Nullable(String),
                miner Nullable(String),
                nonce Nullable(String),
                parent_hash Nullable(String),
                receipts_root Nullable(String),
                sha3_uncles Nullable(String),
                size Nullable(String),
                state_root Nullable(String),
                timestamp Int32,
                total_difficulty Nullable(String),
                transaction_count Nullable(Int32),
                transactions_root Nullable(String),
                type Nullable(String),
                update_at Datetime DEFAULT now(),
                withdrawals Array(Tuple(idx String, validator_index String, address String, amount String))
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(toDateTime(timestamp))
                ORDER BY number
                TTL toDateTime(update_at) + INTERVAL 30 DAY
                SETTINGS index_granularity = 8192;
        """)
        
        logger.info("Creating transactions table...")
        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.transactions (
                block_number Int64,
                hash String,
                block_hash Nullable(String),
                block_timestamp Int64,
                from_address Nullable(String),
                gas Nullable(String),
                gas_price Nullable(String),
                input Nullable(String),
                nonce Nullable(Int32),
                receipt_contract_address Nullable(String),
                receipt_cumulative_gas_used Nullable(Int32),
                receipt_gas_used Nullable(Int32),
                receipt_root Nullable(String),
                receipt_status Nullable(Int16),
                to_address Nullable(String),
                transaction_index Int16,
                type Nullable(String),
                update_at Datetime DEFAULT now(),
                value Nullable(String)
            )
            ENGINE = ReplacingMergeTree()
            PARTITION BY (block_number)
            ORDER BY (block_number, transaction_index)
            TTL toDateTime(update_at) + INTERVAL 30 DAY
            SETTINGS index_granularity = 8192;
        """)

        logger.info("Creating token_transfer table...")
        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.token_transfer
                (
                    block_number Int64,
                    contract_address Nullable(String),
                    log_index Int16,
                    from_address Nullable(String),
                    to_address Nullable(String),
                    transaction_hash Nullable(String),
                    update_at Datetime DEFAULT now(),
                    value Nullable(Float64)
                )
                ENGINE = ReplacingMergeTree()
                PARTITION BY (block_number)
                ORDER BY (block_number, log_index)
                TTL toDateTime(update_at) + INTERVAL 30 DAY
                SETTINGS index_granularity = 8192;
            """)

        logger.info("Creating internal_transactions table...")
        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.internal_transactions
            (
                block_number Int64,
                hash String,
                idx Int16,
                contract_address Nullable(String),
                err_code Nullable(String),
                from_address Nullable(String),
                gas Nullable(String),
                gas_used Nullable(String),
                input Nullable(String),
                is_error Nullable(Int16),
                to_address Nullable(String),
                trace_id Nullable(String),
                type Nullable(String),
                update_at Datetime DEFAULT now(),
                value Nullable(String)
            )
            ENGINE = ReplacingMergeTree()
            PARTITION BY (block_number)
            ORDER BY (block_number, hash, idx)
            TTL toDateTime(update_at) + INTERVAL 30 DAY
            SETTINGS index_granularity = 8192;
        """)

    @staticmethod
    def handle_error(exception):
        logger.error(exception)
    
    def clean_cassandra_data(self, data):
        if not data:
            return []
            
        cleaned_data = []
        
        fields_to_remove = {'bucket_id'}
        
        for record in data:
            cleaned_record = {}
            
            for key, value in record.items():
                if key in fields_to_remove:
                    continue
                    
                # Handle field name mapping and type conversion
                if key in ['number', 'timestamp', 'transaction_count', 'block_number']:
                    if value is not None:
                        cleaned_record[key] = int(value)
                    else:
                        cleaned_record[key] = None
                elif key == 'withdrawals':
                    if value is None or value == 'null':
                        cleaned_record[key] = []
                    elif isinstance(value, list):
                        cleaned_record[key] = value
                    else:
                        cleaned_record[key] = []
                else:
                    cleaned_record[key] = value
                    
            cleaned_data.append(cleaned_record)
        return cleaned_data

    def build_insert_statement_from_entity(self, entity, table):
        fields = list(entity.keys())
        fields_str = ','.join(fields)
        insert_stmt = f'INSERT INTO {self.database}.{table} ({fields_str}) VALUES'
        return insert_stmt, fields

    def upsert_entities(self, entities, table):
        if not entities:
            return

        insert_entity_stmt, fields = self.build_insert_statement_from_entity(entities[0], table)
        data_tuples = [tuple(entity.get(field) for field in fields) for entity in entities]

        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.executemany(insert_entity_stmt, data_tuples)
        except Exception as e:
            logger.warning(f'Failed to insert data into ClickHouse table {table}')
            logger.exception(e)

    def upsert_blocks(self, blocks):
        if not blocks:
            return
        blocks = self.clean_cassandra_data(blocks)
        self.upsert_entities(blocks, 'blocks')
    
    def upsert_transactions(self, transactions):
        if not transactions:
            return
        transactions = self.clean_cassandra_data(transactions)
        self.upsert_entities(transactions, 'transactions')

    def upsert_logs(self, logs):
        if not logs:
            return
        logs = self.clean_cassandra_data(logs)
        self.upsert_entities(logs, 'logs')

    def upsert_token_transfers(self, token_transfers):
        if not token_transfers:
            return
        token_transfers = self.clean_cassandra_data(token_transfers)
        self.upsert_entities(token_transfers, 'token_transfer')

    def upsert_internal_transactions(self, internal_transactions):
        if not internal_transactions:
            return
        internal_transactions = self.clean_cassandra_data(internal_transactions)
        self.upsert_entities(internal_transactions, 'internal_transactions')

