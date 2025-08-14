from clickhouse_sqlalchemy import (
    Table, types
)
from sqlalchemy import Column, MetaData, Table


class ClickHouseTables:
    def __init__(self, database: str = None):
        self.metadata = MetaData(schema=database)

        self.blocks = Table(
                'blocks', self.metadata,
                Column('type', types.String),
                Column('number', types.Int32),
                Column('difficulty', types.Nullable(types.String)), 
                Column('extra_data', types.Nullable(types.String)),  
                Column('gas_limit', types.String),
                Column('gas_used', types.String),
                Column('hash', types.String),
                Column('logs_bloom', types.Nullable(types.String)), 
                Column('miner', types.String),
                Column('nonce', types.Nullable(types.String)),  
                Column('parent_hash', types.String),
                Column('receipts_root', types.Nullable(types.String)), 
                Column('sha3_uncles', types.Nullable(types.String)), 
                Column('size', types.Nullable(types.String)),  
                Column('state_root', types.String),
                Column('timestamp',types.Int32),
                Column('total_difficulty',types.Nullable(types.String)), 
                Column('transaction_count', types.Int32),
                Column('transactions_root', types.String),
                Column('withdrawals', types.Array(types.Tuple(types.String, types.String, types.String, types.String))))

        self.transactions = Table(
                'transactions', self.metadata,
                Column('type', types.String),
                Column('hash', types.String),
                Column('nonce', types.Int32),
                Column('block_hash', types.String),
                Column('block_number', types.Int32),
                Column('block_timestamp', types.Int32),
                Column('transaction_index',types.Int32),
                Column('from_address', types.String),
                Column('to_address', types.Nullable(types.String)), 
                Column('value', types.String),
                Column('gas', types.String),
                Column('gas_price', types.Nullable(types.String)),
                Column('input', types.Nullable(types.String)),
                Column('receipt_contract_address', types.Nullable(types.String)),
                Column('receipt_cumulative_gas_used', types.Int32),
                Column('receipt_gas_used', types.Int32),
                Column('receipt_root', types.Nullable(types.String)),
                Column('receipt_status', types.Int8))

        self.collectors = Table(
                'collectors', self.metadata,
                Column('id', types.String),
                Column('last_updated_at_block_number', types.Nullable(types.Int32)),
                Column('start_extracting_at_block_number', types.Nullable(types.Int32)))
        
        self.token_transfer = Table(
                'token_transfer', self.metadata,
                Column('bucket_id', types.Int32),
                Column('contract_address', types.String),
                Column('block_number', types.Int32),
                Column('from_address', types.String),
                Column('to_address', types.String),
                Column('value', types.Float64),
                Column('log_index', types.Int32),
                Column('transaction_hash', types.String))
        
        self.wrapped_token = Table(
                'wrapped_token', self.metadata,
                Column('bucket_id', types.Int32),
                Column('address', types.String),
                Column('contract_address', types.String),
                Column('block_number', types.Int32),
                Column('wallet', types.String),
                Column('value', types.Float64),
                Column('log_index', types.Int32),
                Column('transaction_hash', types.String),
                Column('event_type',types.String))
        
        self.token_decimals = Table(
                'token_decimals', self.metadata,
                Column('address', types.String),
                Column('decimals', types.Int8))
        
        self.logs = Table(
                'logs', self.metadata,
                Column('bucket_id', types.Int32),
                Column('type', types.String),
                Column('topic0', types.String),
                Column('block_number', types.Int32),
                Column('address', types.String),
                Column('block_hash', types.String),
                Column('data', types.String),
                Column('event_signature', types.String),
                Column('log_index', types.Int32),
                Column('topics', types.Array(types.String)),
                Column('transaction_hash', types.String),
                Column('transaction_index',types.Int32))
        
        self.internal_transactions = Table(
                'internal_transactions', self.metadata,
                Column('bucket_id', types.Int32),
                Column('from_address', types.String),
                Column('to_address', types.String),
                Column('value', types.String),
                Column('contract_address', types.String),
                Column('input', types.String),
                Column('type', types.String),
                Column('gas', types.String),
                Column('gas_used', types.String),
                Column('trace_id', types.String),
                Column('is_error', types.Int8),
                Column('err_code', types.String),
                Column('idx', types.Int32),
                Column('hash', types.String),
                Column('block_number', types.Int32))