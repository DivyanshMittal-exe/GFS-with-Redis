import os

REDIS_HOST = 'localhost'
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)

CHUNK_SIZE = 4 #(in MB)