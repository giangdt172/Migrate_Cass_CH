from database.clickhouse_client import ClickhouseClient

client = ClickhouseClient(connection_url='clickhouse+native://default:123456789@localhost:9000/default', db_prefix='')
