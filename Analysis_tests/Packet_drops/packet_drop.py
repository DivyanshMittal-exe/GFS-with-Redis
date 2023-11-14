import os
import random
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


class read_test(unittest.TestCase):
    def test_read(self):
        total_workers = 5
        
        
        drop_rates = [0.6]
        file_path = os.path.join(current_dir, "write_log.txt")
        # with open(file_path, "w") as log_file:
        #     log_file.write(f"Drop Rate|Timing|IndividualLogs\n")
        
        for drop_rate in drop_rates:

            all_timings = []
            
            workers = [Chunk_Worker(drop_packet=drop_rate) for _ in range(total_workers)]

            for worker in workers:
                worker.make_worker()

            worker_names = [worker.name for worker in workers]

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
                log_file.write(f"{drop_rate}|{end_time-str_time}|{all_timings}\n")

            os.system(f"rm *{DEBUG_DUMP_FILE_SUFFIX}")


if __name__ == "__main__":
    unittest.main()
