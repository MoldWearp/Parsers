from loguru import logger


class KafkaCoreClient:
    def __init__(self, servers):
        if servers is None or servers == '':
            logger.error("The servers mustn't be empty")
        self._servers = servers
