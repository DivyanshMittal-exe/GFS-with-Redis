import os
import pika

REDIS_HOST = 'localhost'
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)

CHUNK_SIZE = 4 #(in MB)
WRITE_SIZE = 1 #(in MB)

LOGFILE = 'GFS.log'
WORKER_COUNT_WITH_CHUNK = 3
LEASE_TIME = 60

TIMEOUT = 2

WORKER_COUNT = 4

PIKA_HOST = 'localhost'
PIKA_CONNECTION_PARAMETERS = pika.ConnectionParameters(host=PIKA_HOST)
CHUNK_EXCHANGE = 'chunk_exchange'

# DEBUG VARIABLES
DEBUG_DUMP_FILE_SUFFIX = 'dump.txt'
WORKER_DUMP_CHUNKS = False

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
    PUT_DATA_OF_A_CHUNK = 'PUT_DATA_OF_A_CHUNK'
    GET_CHUNK = 'get_chunk'
    UPDATE_PRIMARY = 'update_primary'
    WRITE_TO_CHUNK = 'write_to_chunk'
    WRITE_TO_CHUNK_NON_PRIMARY = 'write_to_chunk_non_primary'
    ACK_T0_CHUNK_WRITE = 'ack_to_chunk_write'

class StatusCodes(str, Enum):
    CHUNK_HANDLE_REQUEST_SUCCESSFUL = 'CHUNK_HANDLE_REQUEST_SUCCESSFUL'
    WRITE_SUCCESS = 'WRITE_SUCCESS'
    WRITE_FAILED = 'WRITE_FAILED'
    READ_SUCCESS = 'READ_SUCCESS'
    READ_FAILED = 'READ_FAILED'
    CHUNK_FULL = 'CHUNK_FULL'
    BAD_OFFSET = 'BAD_OFFSET'
    NOT_A_PRIMARY = 'NOT_A_PRIMARY'