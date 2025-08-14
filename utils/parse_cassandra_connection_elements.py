import logging

logger = logging.getLogger('Parser Logger')

def parse_cassandra_connection_elements(string):
    """
    example output for exporter_type: exporter_type@username:password@connection_url

    :param string:
    :return: username, password, connection_url
    """
    try:
        elements = string.split('@')
        auth = elements[1].split(':')
        username = auth[0]
        password = auth[1]
        host_part = elements[2].split(':')
        host, port = host_part[0], host_part[1]
        return host, port, username, password
    except Exception as e:
        logger.warning(f"get_connection_elements err {e}")
        return None, None, None, None