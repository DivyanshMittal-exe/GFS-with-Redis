import ast
import sys
import pika
from GFS.rds import redis
from threading import Event
import time
import logging
from GFS.config import CHUNK_EXCHANGE, DEBUG_DUMP_FILE_SUFFIX, LOGFILE, PIKA_CONNECTION_PARAMETERS, PIKA_HOST, \
    WORKER_COUNT, WORKER_DUMP_CHUNKS, GFSEvent, CHUNK_SIZE, WRITE_SIZE, WORKER_COUNT_WITH_CHUNK, StatusCodes
import os
from threading import Thread
import signal

def find_index_to_write(lst):
    for i in range(len(lst) - 1, -1, -1):
        if lst[i] is not None:
            if i == len(lst) - 1:
                return None
            return i + 1

    return 0

def worker_child_handler(signum, frame):
    while True:
        try:
            # Try to reap (collect information about) exited child processes
            # This will prevent zombie processes
            pid, status = os.waitpid(-1, os.WNOHANG)
        except OSError:
            break
        if pid == 0:
            break
        logging.info(f'Child process {pid} exited with status {status}')
        print(f"Child process {pid} exited with status {status}")

class Chunk_Worker:
    
    def __init__(self) -> None:
        self.name = "ChunkWorker-XXXX"
        self.pid = -1
        
        self.connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=CHUNK_EXCHANGE, exchange_type='topic')
        
        self.queue = self.channel.queue_declare(queue='', exclusive=True)        
    
        self.chunk_exchange_event = Event()
        self.cardiac_arrest_event = Event()
        self.worker_thread_event = Event() 
        
        self.write_data_in_memory = {}

        self.persistent_chunks = {}

        self.ack_status = {}
        self.ack_to_client_queue = {}

    
    def dump_memory(self) -> None:
        with open(self.name + DEBUG_DUMP_FILE_SUFFIX, 'w') as f:
            f.write(str(self.write_data_in_memory))
    
    def breathe(self, event: Event) -> None:
        self.heart = redis.Heart(self.name)
        while True:
            self.heart.beat()
            time.sleep(1) 
            if event.is_set():
                break
            
        print(f"{self.name} is dead.")
    
    def kill(self) -> None:
        logging.info(f"Killing {self.name}")
        print(f"Killing {self.name}")    
        os.kill(self.pid, signal.SIGKILL)

    def handle_errors(self, status_code: StatusCodes, **kwargs) -> None:
        if status_code == StatusCodes.NOT_A_PRIMARY:
            properties = kwargs['properties']
            request_id = kwargs['request_id']
            self.channel.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                body=b'',
                properties=pika.BasicProperties(
                    headers={'request_id': request_id,
                             'type': GFSEvent.ACK_T0_CHUNK_WRITE,
                             'status': StatusCodes.NOT_A_PRIMARY}
                    )
                )
        elif status_code == StatusCodes.CHUNK_FULL:
            properties = kwargs['properties']
            request_id = kwargs['request_id']
            self.channel.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                body=b'',
                properties=pika.BasicProperties(
                    headers={'request_id': request_id,
                             'type': GFSEvent.ACK_T0_CHUNK_WRITE,
                             'status': StatusCodes.CHUNK_FULL}
                    )
                )


    def chunk_exchange(self, event: Event) -> None:
        for method_frame, properties, body in self.channel.consume(queue=self.queue.method.queue):

            header = properties.headers

            if header['type'] == GFSEvent.GET_CHUNK:
                key = header['key']
                # TODO: Handle case when key is not here
                chunk_to_return_list = self.persistent_chunks[key]

                data = ''

                for chunk in chunk_to_return_list:
                    if chunk is not None:
                        data += chunk.decode()




                self.channel.basic_ack(method_frame.delivery_tag)
                self.channel.basic_publish( exchange='',
                                            routing_key=properties.reply_to,
                                            body=data.encode(),
                                            properties=pika.BasicProperties(headers={'key': key}))


            elif header['type'] == GFSEvent.PUT_CHUNK:
                key = header['key']
                self.write_data_in_memory[key] = body
                self.channel.basic_ack(method_frame.delivery_tag)

                if event.is_set():
                    break

                if WORKER_DUMP_CHUNKS:
                    self.dump_memory()

            elif header['type'] == GFSEvent.WRITE_TO_CHUNK:

                chunk_key = header['chunk_key']
                data_key = header['data_key']
                request_id = header['request_id']

                self.ack_to_client_queue[request_id] = properties.reply_to


                current_primary, time_to_expire = redis.get_primary(chunk_key)
                if current_primary != self.name or time_to_expire <= time.perf_counter():
                    kwargs = {
                            'properties': properties,
                            'request_id': request_id
                        }
                    self.handle_errors(StatusCodes.NOT_A_PRIMARY, kwargs)
                    continue

                if chunk_key not in self.persistent_chunks:
                    self.persistent_chunks[chunk_key] = [None] * (CHUNK_SIZE // WRITE_SIZE)

                current_state_of_chunk = self.persistent_chunks[chunk_key]

                offset_to_write_at = find_index_to_write(current_state_of_chunk)

                if offset_to_write_at is None:
                    kwargs = {
                        'properties': properties,
                        'request_id': request_id
                        }
                    self.handle_errors(StatusCodes.CHUNK_FULL, kwargs)
                    continue

                current_state_of_chunk[offset_to_write_at] = self.write_data_in_memory[data_key]
                self.persistent_chunks[chunk_key] = current_state_of_chunk

                workers_whom_to_send = redis.get_servers(chunk_key)

                workers_whom_to_send.remove(self.name)

                routing_key = f".{'.'.join(workers_whom_to_send)}."

                self.ack_status[request_id] = 1

                self.channel.basic_publish(
                    exchange=CHUNK_EXCHANGE,
                    routing_key=routing_key,
                    body=b'',
                    properties=pika.BasicProperties(
                        headers={'chunk_key': chunk_key,
                                 'data_key': data_key,
                                 'request_id': request_id,
                                 'offset': offset_to_write_at,
                                 'type': GFSEvent.WRITE_TO_CHUNK_NON_PRIMARY},
                        reply_to=self.queue.method.queue
                        )
                    )

            elif header['type'] == GFSEvent.WRITE_TO_CHUNK_NON_PRIMARY:

                chunk_key = header['chunk_key']
                data_key = header['data_key']
                request_id = header['request_id']
                offset = header['offset']



                if chunk_key not in self.persistent_chunks:
                    self.persistent_chunks[chunk_key] = [None] * (CHUNK_SIZE // WRITE_SIZE)

                current_state_of_chunk = self.persistent_chunks[chunk_key]

                current_state_of_chunk[offset] = self.write_data_in_memory[data_key]
                self.persistent_chunks[chunk_key] = current_state_of_chunk



                self.channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    body=b'True',
                    properties=pika.BasicProperties(
                        headers={'request_id': request_id,
                                 'type': GFSEvent.ACK_T0_CHUNK_WRITE},
                    )
                )

            elif header['type'] == GFSEvent.ACK_T0_CHUNK_WRITE:
                request_id = header['request_id']

                body = body.decode()
                if body == 'True':
                    self.ack_status[request_id] += 1



                if self.ack_status[request_id] == WORKER_COUNT_WITH_CHUNK:
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.ack_to_client_queue[request_id],
                        body=b'',
                        properties=pika.BasicProperties(
                            headers={'request_id': request_id,
                                     'type': GFSEvent.ACK_T0_CHUNK_WRITE,
                                     'status': StatusCodes.WRITE_SUCCESS}
                        )
                    )








        requeued_messages = self.channel.cancel()
        print('Requeued %i messages' % requeued_messages)

        self.channel.close()
        self.connection.close()
                    
        
    def work(self, event: Event) -> None:
        while True:
            time.sleep(1)
            
    
    def make_worker(self) -> None:  
        pid = os.fork()
        
        if pid < 0:
            logging.error("Failed to fork")
            return

        if pid == 0:
            
            self.pid = os.getpid()
            self.name = self.name.replace("XXXX", str(self.pid))
            
            self.channel.queue_bind(exchange=CHUNK_EXCHANGE,
                                queue=self.queue.method.queue,
                                routing_key=f"#.{self.name}.#")
            
            worker_thread = Thread(target=self.work, args=(self.worker_thread_event,))  
            heart_thread = Thread(target=self.breathe, args=(self.cardiac_arrest_event,))
            chunk_exchange_thread = Thread(target=self.chunk_exchange, args=(self.chunk_exchange_event,))
            
            worker_thread.start()
            heart_thread.start()
            chunk_exchange_thread.start()
            
            worker_thread.join()
            self.cardiac_arrest_event.set()
            heart_thread.join()
            self.chunk_exchange_event.set()
            chunk_exchange_thread.join()
            
            self.kill()
        else:
            self.pid = pid
            self.name = self.name.replace("XXXX", str(self.pid))
            signal.signal(signal.SIGCHLD, worker_child_handler)
            
            
def sigterm_kill_handler():
    for worker in workers:
        worker.kill()
    
    logging.info("All Chunk Workers killed")
    sys.exit(0)



if __name__ == "__main__":

    logging.basicConfig(filename=LOGFILE, level=logging.DEBUG)
    logging.info("Starting Chunk Workers")

    workers = [Chunk_Worker() for _ in range(WORKER_COUNT)]

    signal.signal(signal.SIGTERM, sigterm_kill_handler)

    for worker in workers:
        worker.make_worker()

    logging.info("All Chunk Workers started")

    while True:
        pid, status = os.wait()
        for worker in workers:
            if worker.pid == pid:
                logging.info(f"Child process {pid} exited with status {status}")
                print(f"Child process {pid} exited with status {status}")
                worker.make_worker()
                break