import os
import random
import time
import unittest

import pika

from GFS.chunk import ChunkHandle
from GFS.chunk_workers.worker import Chunk_Worker
from GFS.client import GFSClient
from GFS.config import CHUNK_EXCHANGE, PIKA_CONNECTION_PARAMETERS, DEBUG_DUMP_FILE_SUFFIX, GFSEvent
from GFS.server import GFS_Server
import secrets

class read_test(unittest.TestCase):
  def test_read(self):
    total_workers = 5

    workers = [Chunk_Worker() for _ in range(total_workers)]



    for worker in workers:
      worker.make_worker()

    worker_names = [worker.name for worker in workers]

    # message = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 10))

    message = secrets.token_bytes(1024*1024)
    print(worker_names)


    client = GFSClient()

    total_write_times = 20
    timings_log = []

    with GFS_Server(worker_names) as server:
      filename = 'abc'
      offset = 0

      str_time = time.time()

      for i in range(total_write_times):
        return_of_write = client.write(filename, i//4, message)
        print(f'For iteration {i}, write returned {return_of_write}')

        end_time = time.time()
        timings_log.append([i, end_time - str_time])

    for worker in workers:
      worker.kill()

    with open("write_log.txt", "w") as log_file:
      for entry in timings_log:
        log_file.write(f"{entry[0]}, {entry[1]}\n")

    os.system(f'rm *{DEBUG_DUMP_FILE_SUFFIX}')


if __name__ == "__main__":
  unittest.main()