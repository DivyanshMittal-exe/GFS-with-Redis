import os
import random
import unittest

import pika

from GFS.chunk import ChunkHandle
from GFS.chunk_workers.worker import Chunk_Worker
from GFS.client import GFSClient
from GFS.config import CHUNK_EXCHANGE, PIKA_CONNECTION_PARAMETERS, DEBUG_DUMP_FILE_SUFFIX
from GFS.server import GFS_Server


class read_test(unittest.TestCase):

  def test_read(self):
    filename = "test_file_name.txt"
    offset = '0'
    total_workers = 5
    no_of_workers_to_send = total_workers // 2

    workers = [Chunk_Worker() for _ in range(total_workers)]



    for worker in workers:
      worker.make_worker()

    worker_names = [worker.name for worker in workers]

    chosen_workers = random.sample(worker_names, no_of_workers_to_send)

    chunk_handle_is = ChunkHandle(servers=chosen_workers, primary=chosen_workers[0], lease_time=10)

    message = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 10))

    connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)

    routing_key = f".{'.'.join(chosen_workers)}."

    channel = connection.channel()
    channel.basic_publish(exchange=CHUNK_EXCHANGE, routing_key=routing_key, body=message,
                          properties=pika.BasicProperties(headers={'key': str(chunk_handle_is.chunk_uid), 'type': 'PUT'}))
    connection.close()

    client = GFSClient()

    with GFS_Server() as server:

      server.file_to_chunk_handles[filename][offset] = chunk_handle_is

      data = client.read(filename, offset)

      data = data.decode()
      self.assertEqual(data, message)

    for worker in workers:
      worker.kill()

    os.system(f'rm *{DEBUG_DUMP_FILE_SUFFIX}')


if __name__ == "__main__":
  unittest.main()