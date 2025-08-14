import logging
import pathlib
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from configs.config import MonitoringConfig

def logging_basic_config(filename=None):
    format = '%(asctime)s - %(name)s [%(levelname)s] - %(message)s'
    if filename is not None:
        logging.basicConfig(level=logging.INFO, format=format, filename=filename)
    else:
        logging.basicConfig(level=logging.INFO, format=format)

def write_monitor_logs(stream_name, synced_block, chain_id):
    monitor_path = MonitoringConfig.MONITOR_ROOT_PATH
    registry = CollectorRegistry()
    g = Gauge('last_block_synced', 'Block synced', ['process', 'chain_id'], registry=registry)
    g.labels(stream_name, chain_id).inc(synced_block)
    _file = monitor_path + stream_name + '.prom'
    write_to_textfile(_file, registry)