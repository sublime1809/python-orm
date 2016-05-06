import re
import logging
import time
import json

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure

from settings import mongodb as db_settings

log = logging.getLogger(__name__)

HEALTH_CHECK_INTERVAL = 1


class MongoConnector:
    camel_case_regex = re.compile('(.)([A-Z][a-z]+)')
    snake_case_regex = re.compile('([a-z0-9])([A-Z])')
    mongo_client = None
    last_health_check_time = time.time()

    REQUIRED_SETTINGS = ('MONGO_URI', 'MONGO_DATABASE')

    def __init__(self):
        for setting in self.REQUIRED_SETTINGS:
            if not hasattr(db_settings, setting):
                raise ConnectionAbortedError('Must set {} in project '
                                             'settings.'.format(setting))

    @classmethod
    def _isMaster(cls):
        # http://docs.mongodb.org/manual/reference/command/isMaster/
        if cls.mongo_client:
            try:
                isMaster = cls.mongo_client['admin'].command('isMaster')
                master_status = isMaster.get('ismaster')
                if master_status:
                    return master_status
                log.info('MongoDB not connected to master server: {}'.
                         format(json.dumps(isMaster)))
            except Exception as e:
                log.info("Exception in {}._isMaster().  Returning False. "
                         "{}: {}".format(cls.__name__, e.__class__.__name__, e))
        return False

    @classmethod
    def get_connection(cls, retries=10):
        try:
            if cls.mongo_client is None or not cls.mongo_client.alive():
                cls.mongo_client = MongoClient(db_settings.MONGO_URI)
                log.info('New Mongo connection')
            if time.time() > (cls.last_health_check_time +
                              HEALTH_CHECK_INTERVAL):
                # will retry connect if not connected to a master
                cls.last_health_check_time = time.time()
                if not cls._isMaster():
                    cls.mongo_client = None
                    return cls.get_connection()
        except (ConnectionFailure, AutoReconnect) as e:
            cls.mongo_client = None
            if retries > 0:
                log.info('Retry mongo connection. '
                         '{}: {}'.format(e.__class__.__name__, e))
                time.sleep(0.5)
                return cls.get_connection(retries=(retries-1))
            log.error("Persistent errors while trying to get mongo connection."
                      "{}: {}".format(e.__class__.__name__, e))
            raise e
        return cls.mongo_client

    @classmethod
    def get_database(cls):
        return cls.get_connection()[db_settings.MONGO_DATABASE]

    @classmethod
    def get_table(cls, klass):
        if hasattr(klass, '__name__'):
            name = klass.__name__
        else:
            name = klass.__class__.__name__
        s1 = cls.camel_case_regex.sub(r'\1_\2', name)
        table_name = cls.snake_case_regex.sub(r'\1_\2', s1).lower()
        return cls.get_database()[table_name]

    @classmethod
    def drop_database(cls):
        """
        This is to clear out the database -- should only be used for tests
        """
        cls.get_connection().drop_database(cls.get_database())
