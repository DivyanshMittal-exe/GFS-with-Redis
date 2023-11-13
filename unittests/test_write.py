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


class read_test(unittest.TestCase):

  def test_read(self):
    total_workers = 5

    workers = [Chunk_Worker() for _ in range(total_workers)]



    for worker in workers:
      worker.make_worker()

    worker_names = [worker.name for worker in workers]

    message = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 10))
    print(worker_names)

    client = GFSClient()

    with GFS_Server(worker_names) as server:
      filename = 'abc'
      offset = 0


      client.write(filename, offset, message)


      time.sleep(1)

      status = False
      while not status:
        status, data, params = client.read(filename, offset)
        print(f'Recieved {data}, with {status} and params {params}')

      data = data.decode()
      print(data)
      self.assertEqual(data, message)

    for worker in workers:
      worker.kill()

    os.system(f'rm *{DEBUG_DUMP_FILE_SUFFIX}')


if __name__ == "__main__":
  unittest.main()