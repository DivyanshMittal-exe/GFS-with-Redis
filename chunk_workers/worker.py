from rds import redis
from threading import Event
import time
import logging
from config import LOGFILE
import os
from threading import Thread
import signal

class Chunk_Worker:
    
    def __init__(self) -> None:
        self.name = "Chunk Worker-XXXX"
        self.heart = redis.Heart(self.name)
                
    
    def breathe(self, event: Event) -> None:
        while True:
            self.heart.beat()
            time.sleep(1)
            
            if event.is_set():
                break
            
        print(f"{self.name} is dead.")
    
    def chunk_exchange(self, event: Event) -> None:
        pass
        
    def work(self) -> None:
        pass
    
    def make_worker(self) -> None:
        pid = os.fork()
        
        if pid < 0:
            logging.error(f"Failed to fork ")
            return

        if pid == 0:
            
            self.name = self.name.replace("XXXX", str(os.getpid()))
            
            cardiac_arrest = Event()
            chunk_exchange_event = Event()
            
            worker_thread = Thread(target=self.work)
            heart_thread = Thread(target=self.breathe, args=(cardiac_arrest,))
            chunk_exchange_thread = Thread(target=self.chunk_exchange, args=(chunk_exchange_event,))
            
            worker_thread.start()
            heart_thread.start()
            chunk_exchange_thread.start()
            
            worker_thread.join()
            cardiac_arrest.set()
            heart_thread.join()
            chunk_exchange_event.set()
            chunk_exchange_thread.join()
            
            logging.info(f"{self.name} is dead.")
            os.kill(os.getpid(), signal.SIGKILL)
            
            
            
        