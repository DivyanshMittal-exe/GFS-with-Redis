
import os
import time
import unittest
import pika
import random
from GFS.chunk_workers.worker import Chunk_Worker
from GFS.config import CHUNK_EXCHANGE, DEBUG_DUMP_FILE_SUFFIX, PIKA_CONNECTION_PARAMETERS

class chunk_distribution_test(unittest.TestCase):


    def test_chunk_distribution(self):
        total_workers = 5
        no_of_workers_to_send = total_workers // 2

        workers = [Chunk_Worker() for _ in range(total_workers)]

        for worker in workers:
            worker.make_worker()
            
            
        worker_names = [worker.name for worker in workers]
        message = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 10))
        key = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz', 4))
        
        
        chosen_workers = random.sample(worker_names, no_of_workers_to_send)

        connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
        
        routing_key = f".{'.'.join(chosen_workers)}."
        
        
        channel = connection.channel()
        channel.basic_publish(exchange=CHUNK_EXCHANGE,
                                routing_key=routing_key,
                                body=message,
                                properties=pika.BasicProperties(headers={'key': key, 'type': 'PUT'}))
        connection.close()
            
        time.sleep(1)
        
        for worker in workers:
            worker.kill()

        for worker in workers:
            if worker.name in chosen_workers:  
                with open(worker.name + DEBUG_DUMP_FILE_SUFFIX, 'r') as f:
                    worker_memory = f.readlines()           
                message_as_dict = f"{{'{key}': b'{message}'}}"   
                self.assertEqual(worker_memory[0], message_as_dict)
        
        os.system(f'rm *{DEBUG_DUMP_FILE_SUFFIX}')