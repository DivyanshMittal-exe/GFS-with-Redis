import ast
import sys
import pika
from GFS.rds import redis
from threading import Event
import time
import logging
from GFS.config import CHUNK_EXCHANGE, DEBUG_DUMP_FILE_SUFFIX, LOGFILE, PIKA_CONNECTION_PARAMETERS, PIKA_HOST, WORKER_COUNT, WORKER_DUMP_CHUNKS
import os
from threading import Thread
import signal


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
        self.heart = redis.Heart(self.name)
        self.pid = -1
        
        self.connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=CHUNK_EXCHANGE, exchange_type='topic')
        
        self.queue = self.channel.queue_declare(queue='', exclusive=True)        
    
        self.chunk_exchange_event = Event()
        self.cardiac_arrest_event = Event()
        self.worker_thread_event = Event() 
        
        self.chunks_in_memory = {}
    
    def dump_memory(self) -> None:
        with open(self.name + DEBUG_DUMP_FILE_SUFFIX, 'w') as f:
            f.write(str(self.chunks_in_memory))
    
    def breathe(self, event: Event) -> None:
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
    
    def chunk_exchange(self, event: Event) -> None:
        for method_frame, _, body in self.channel.consume(queue=self.queue.method.queue):
            
            message_dict = ast.literal_eval(body.decode('utf-8'))
            
            self.chunks_in_memory.update(message_dict)
            
            self.channel.basic_ack(method_frame.delivery_tag)
            
            if event.is_set():
                break
        
            if WORKER_DUMP_CHUNKS:
                self.dump_memory()
        
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