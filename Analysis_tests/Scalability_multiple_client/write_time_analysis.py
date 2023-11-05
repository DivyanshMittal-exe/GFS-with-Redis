import os
import random
import threading
import time
import unittest

import sys
import os

import numpy as np

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
root_dir = os.path.abspath(os.path.join(root_dir, os.pardir))
sys.path.append(root_dir)

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
    total_clients_to_use = [2 * i for i in range(1, 20)]

    file_path = os.path.join(current_dir, f"read_log_multiple_clients.txt")
    with open(file_path, "w") as log_file:
      log_file.write(f"")

    for worker in workers:
      worker.make_worker()

    worker_names = [worker.name for worker in workers]

    # message = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 10))

    message = secrets.token_bytes(1024 * 1024)
    print(worker_names)

    client = GFSClient()

    total_write_times = 40
    timings_log = []

    with GFS_Server(worker_names) as server:
      filename = 'abc'
      offset = 0

      for i in range(total_write_times):
        return_of_write = client.write(filename, i // 4, message)
        print(f'For iteration {i}, write returned {return_of_write}')

      client_threads = []

      print('Starting read')

      def read_and_log_time(client_id):
        client_n = GFSClient()
        start_time = time.time()
        for i in range(10):
          data = client_n.read(filename, i)
          # print(data[:10])
        end_time = time.time()
        timings_log.append([end_time - start_time])

      for total_clients in total_clients_to_use:

        for client_id in range(total_clients):
          thread = threading.Thread(target=read_and_log_time, args=(client_id,))
          client_threads.append(thread)
          thread.start()

        for thread in client_threads:
          thread.join()

        mean_read_time = np.mean(np.array(timings_log))

        with open(file_path, "a") as log_file:
          log_file.write(f"{total_clients}| {mean_read_time}| {timings_log} \n")

        timings_log = []

    for worker in workers:
      worker.kill()

    os.system(f'rm *{DEBUG_DUMP_FILE_SUFFIX}')


if __name__ == "__main__":
  unittest.main()