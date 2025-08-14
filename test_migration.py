#!/usr/bin/env python3

import logging
import sys
import time

# Cấu hình logging chi tiết
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def test_migration():
    try:
        logger.info("🚀 Starting block migration test...")
        
        # Import các modules cần thiết
        from database.cassandra_client import CassandraClient
        from database.clickhouse_client import ClickhouseClient
        from streaming.export_blocks_adapter import ExportBlocksAdapter
        from streaming.export_transactions_adapter import ExportTransactionsAdapter
        from streaming.export_transfer_adapter import ExportTransferAdapter
        # Tạo kết nối Cassandra
        logger.info("📡 Connecting to Cassandra...")
        cassandra_client = CassandraClient(
            connection_url='',
            keyspace='blockchain_etl'
        )
        logger.info("✅ Cassandra connection established")
        
        # Tạo kết nối ClickHouse
        logger.info("🗄️ Connecting to ClickHouse...")
        clickhouse_client = ClickhouseClient(
            connection_url='clickhouse+native://default:123456789@localhost:9000/default',
            db_prefix='test'
        )
        logger.info("✅ ClickHouse connection established")
        
        # Test với range nhỏ để kiểm tra
        start_block = 51680896
        end_block = 51680906  # Chỉ test 5 blocks
        
        logger.info(f"📊 Testing migration for blocks {start_block} to {end_block}")
        
        # Tạo adapter
        adapter = ExportTransferAdapter(
            batch_size=10,
            max_workers=2,
            collector_id=None,
            item_importer=cassandra_client,
            item_exporter=clickhouse_client
        )
        
        # Bắt đầu migration
        start_time = time.time()
        logger.info("🔄 Starting block export...")
        
        adapter.export_all(start_block, end_block)
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"✅ Migration completed successfully!")
        logger.info(f"⏱️ Time taken: {duration:.2f} seconds")
        logger.info(f"📈 Migrated {end_block - start_block + 1} blocks")
        
        # # Kiểm tra kết quả trong ClickHouse
        # logger.info("🔍 Verifying migrated data...")
        # result = clickhouse_client.execute_query(
        #     f"SELECT COUNT(*) as count FROM test_blockchain_etl.blocks WHERE number >= {start_block} AND number <= {end_block}"
        # )
        
        # if result:
        #     count = list(result)[0][0]
        #     logger.info(f"📊 Found {count} blocks in ClickHouse")
            
        #     if count > 0:
        #         # Hiển thị một vài blocks mẫu
        #         sample_result = clickhouse_client.execute_query(
        #             f"SELECT number, hash, timestamp FROM test_blockchain_etl.blocks WHERE number >= {start_block} AND number <= {end_block} LIMIT 3"
        #         )
        #         if sample_result:
        #             logger.info("📋 Sample migrated blocks:")
        #             for row in sample_result:
        #                 logger.info(f"  Block {row[0]}: {row[1]} (timestamp: {row[2]})")
        
        # logger.info("🎉 Migration test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Migration test failed: {e}")
        logger.exception("Full error details:")
        return False
    
    return True

if __name__ == "__main__":
    success = test_migration()
    if not success:
        sys.exit(1) 