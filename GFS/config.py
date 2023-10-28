import os
import pika

REDIS_HOST = 'localhost'
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)

CHUNK_SIZE = 4 #(in MB)
WRITE_SIZE = 1 #(in MB)

LOGFILE = 'GFS.log'
WORKER_COUNT_WITH_CHUNK = 3
LEASE_TIME = 60

WORKER_COUNT = 4

PIKA_HOST = 'localhost'
PIKA_CONNECTION_PARAMETERS = pika.ConnectionParameters(host=PIKA_HOST)
CHUNK_EXCHANGE = 'chunk_exchange'

# DEBUG VARIABLES
DEBUG_DUMP_FILE_SUFFIX = 'dump.txt'
WORKER_DUMP_CHUNKS = True

SERVER_REQUEST_QUEUE = 'server_request_queue'
SERVER_REPLY_QUEUE = 'server_reply_queue'
SERVER_REQUEST_EXCHANGE = ''
SERVER_REPLY_EXCHANGE = ''

def get_key(filename, offset):
    return f'{filename}|{offset}'

def get_filename_and_offset(key):
    filename, offset =  key.split('|')
    return filename, int(offset)

from enum import Enum, auto

class GFSEvent(str, Enum):
    PUT_CHUNK = 'put_chunk'
    GET_CHUNK = 'get_chunk'
    UPDATE_PRIMARY = 'update_primary'
    WRITE_TO_CHUNK = 'write_to_chunk'
    WRITE_TO_CHUNK_NON_PRIMARY = 'write_to_chunk_non_primary'
    ACK_T0_CHUNK_WRITE = 'ack_to_chunk_write'
