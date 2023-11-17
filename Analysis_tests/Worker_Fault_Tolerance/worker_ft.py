import os
import random
import threading
import time
import unittest

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
root_dir = os.path.abspath(os.path.join(root_dir, os.pardir))
sys.path.append(root_dir)

import pika
from GFS.chunk import ChunkHandle
from GFS.chunk_workers.worker import Chunk_Worker
from GFS.client import GFSClient
from GFS.config import (
    CHUNK_EXCHANGE,
    PIKA_CONNECTION_PARAMETERS,
    DEBUG_DUMP_FILE_SUFFIX,
    GFSEvent, StatusCodes,
    )
from GFS.server import GFS_Server
import secrets
import signal


random.seed(42)

class read_test(unittest.TestCase):
    def test_read(self):
        total_workers = 5
        
        
        file_path = os.path.join(current_dir, "write_log.txt")
        # with open(file_path, "w") as log_file:
        #     log_file.write(f"Drop Rate|Timing|IndividualLogs\n")
        
        all_timings = []

        die_prob = 0.8

        workers = [Chunk_Worker(die_randomly=die_prob) for _ in range(total_workers)]

        for worker in workers:
            worker.make_worker()

        worker_names = [worker.name for worker in workers]


        # Create a dictionary to store the status of each worker


        # def worker_management_thread():
        #     worker_status = {worker.name: "alive" for worker in workers}
        #
        #
        #     # time.sleep(4)
        #
        #     while True:
        #         # Randomly select a worker
        #         worker_to_manage = random.choice(workers)
        #
        #         # Check the status of the selected worker
        #         if worker_status[worker_to_manage.name] == "alive":
        #             # If alive, kill it
        #             os.kill(worker_to_manage.pid, signal.SIGSTOP)
        #             worker_status[worker_to_manage.name] = "dead"
        #             print(f"Worker {worker_to_manage.name} killed.")
        #         else:
        #             # If dead, revive it
        #             os.kill(worker_to_manage.pid, signal.SIGCONT)
        #             worker_status[worker_to_manage.name] = "alive"
        #             print(f"Worker {worker_to_manage.name} revived.")
        #
        #         # Sleep for a random amount of time
        #         sleep_time = random.uniform(1, 10)  # Adjust the range as needed
        #         time.sleep(sleep_time)
        #
        # worker_management_thread = threading.Thread(target=worker_management_thread)
        # worker_management_thread.start()

        message = secrets.token_bytes(1024 * 1024)
        print(worker_names)

        client = GFSClient()

        total_write_times = 20
        timings_log = []

        with GFS_Server(worker_names) as server:
            filename = "abc"
            offset = 0
            str_time = time.time()

            for i in range(total_write_times):
                return_of_write = StatusCodes.WRITE_FAILED

                while return_of_write != StatusCodes.WRITE_SUCCESS:
                    return_of_write = client.write(filename, offset, message)

                    print(f"Write returned {return_of_write} for iteration {i}")

                    if return_of_write == StatusCodes.WRITE_FAILED:
                        print(f"Write failed for iteration {i}")

                    if return_of_write == StatusCodes.CHUNK_FULL:
                        print(f"Chunk full for iteration {i}")
                        offset += 1


                print(f"SUCCESS: For iteration {i}, write returned {return_of_write}")
                all_timings.append(time.time() - str_time)

            end_time = time.time()
            server.stop()

        # timings_log.append([drop_rate, end_time - str_time])

        for worker in workers:
            worker.kill()


        with open(file_path, "a") as log_file:
            log_file.write(f"{die_prob}|{end_time-str_time}|{all_timings}\n")

        os.system(f"rm *{DEBUG_DUMP_FILE_SUFFIX}")


if __name__ == "__main__":
    unittest.main()
