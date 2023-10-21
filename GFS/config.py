import os
import pika

REDIS_HOST = 'localhost'
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)

CHUNK_SIZE = 4 #(in MB)

LOGFILE = 'GFS.log'

WORKER_COUNT = 4

PIKA_HOST = 'localhost'
PIKA_CONNECTION_PARAMETERS = pika.ConnectionParameters(host=PIKA_HOST)
CHUNK_EXCHANGE = 'chunk_exchange'

# DEBUG VARIABLES
DEBUG_DUMP_FILE_SUFFIX = 'dump.txt'
WORKER_DUMP_CHUNKS = True