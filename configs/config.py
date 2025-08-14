import os

from dotenv import load_dotenv

load_dotenv()


class CassandraConfig:
    CONNECTION_URL = os.environ.get('CASSANDRA_CONNECTION_URL')
    HOST = os.environ.get('CASSANDRA_NODE_IP')
    PORT = os.environ.get('CASSANDRA_PORT') or 9042
    USERNAME = os.environ.get('CASSANDRA_USERNAME')
    PASSWORD = os.environ.get('CASSANDRA_PASSWORD')
    KEYSPACE = 'blockchain_etl'

class ClickhouseConfig:
    CONNECTION_URL = os.environ.get('CLICKHOUSE_CONNECTION_URL')
    HOST = os.environ.get('CLICKHOUSE_HOST')
    PORT = os.environ.get('CLICKHOUSE_PORT') or 9000
    USERNAME = os.environ.get('CLICKHOUSE_USERNAME')
    PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')
    DATABASE = os.environ.get('CLICKHOUSE_DATABASE') or 'blockchain_etl'

class MonitoringConfig:
    MONITOR_ROOT_PATH = "/home/monitor/.log/"