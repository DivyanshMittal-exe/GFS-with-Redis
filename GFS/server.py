import pickle
import random
import time
from collections import defaultdict
from threading import Event, Thread
from typing import List

import pika

from GFS.chunk import ChunkHandle
from GFS.config import PIKA_CONNECTION_PARAMETERS, SERVER_REPLY_EXCHANGE, SERVER_REPLY_QUEUE, SERVER_REQUEST_QUEUE, \
  get_filename_and_offset, WORKER_COUNT_WITH_CHUNK, LEASE_TIME, StatusCodes
from GFS.rds.redis import set_primary, is_alive, set_lease, get_primary


class GFS_Server:

  def __init__(self, worker_names: List[str]) -> None:
    self.file_to_chunk_handles = defaultdict(dict)
    self.chunk_handle_to_metadata = defaultdict(ChunkHandle)
    self.chunk_handle_event = Event()
    self.lease_update_event = Event()
    self.worker_names = worker_names

  def handle_errors(self, status_code: StatusCodes, **kwargs):
    if status_code == StatusCodes.BAD_OFFSET:
      print(f'Bad offset {kwargs["offset"]}')

      channel = kwargs['channel']
      key = kwargs['key']

      channel.basic_publish(exchange=SERVER_REPLY_EXCHANGE, routing_key=properties.reply_to,
                          body=chunk_handle_serialised, properties=pika.BasicProperties(headers={'key': key}))




    else:
      raise NotImplementedError

  def listen_for_chunk_requests(self, event: Event) -> None:
    connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
    channel = connection.channel()
    request_queue = channel.queue_declare(queue=SERVER_REQUEST_QUEUE, exclusive=False)
    channel.queue_declare(queue=SERVER_REPLY_QUEUE, exclusive=False)
    # channel.exchange_declare(exchange=SERVER_REQUEST_EXCHANGE)
    # channel.exchange_declare(exchange=SERVER_REPLY_EXCHANGE)

    while True:
      for method_frame, properties, body in channel.consume(queue=request_queue.method.queue, auto_ack=True,
                                                            inactivity_timeout=5):

        if method_frame is None:
          break

        key = body.decode()

        filename, offset = get_filename_and_offset(key)

        if filename not in self.file_to_chunk_handles or offset not in self.file_to_chunk_handles[filename]:

          if filename not in self.file_to_chunk_handles:
            if offset != 0:
              kwargs = {'offset': 0, 'channel': channel, 'key': key}
              self.handle_errors(StatusCodes.BAD_OFFSET, **kwargs)
              continue

            self.file_to_chunk_handles[filename] = {}
          else:
            if offset - 1 not in self.file_to_chunk_handles[filename]:
              last_offset_in_mem = max(self.file_to_chunk_handles[filename].keys())
              kwargs = {'offset': last_offset_in_mem, 'channel': channel, 'key':key}
              self.handle_errors(StatusCodes.BAD_OFFSET, **kwargs)
              continue

          workers_that_will_have_the_chunk = random.sample(self.worker_names, WORKER_COUNT_WITH_CHUNK)

          chunk_handle = ChunkHandle(servers=workers_that_will_have_the_chunk,
                                     primary=workers_that_will_have_the_chunk[0],
                                     lease_time=LEASE_TIME)

          set_primary(chunk_handle)

          self.file_to_chunk_handles[filename][offset] = chunk_handle

        chunk_handle = self.file_to_chunk_handles[filename][offset]
        chunk_handle_serialised = pickle.dumps(chunk_handle)

        channel.basic_publish(exchange=SERVER_REPLY_EXCHANGE, routing_key=properties.reply_to,
                              body=chunk_handle_serialised, properties=pika.BasicProperties(headers={'key': key}))

      if event.is_set():
        break

      requeued_messages = channel.cancel()
      print('Requeued %i messages' % requeued_messages)

    channel.close()
    connection.close()

  def handle_lease(self, lease_update_event:Event ):

    while not lease_update_event.is_set():
      min_lease_time = float('inf')
      chunk_with_min_lease_time = None

      for file_to_chunk_dict in self.file_to_chunk_handles.values():
        for chunk_handle in file_to_chunk_dict.values():
          primary, time_to_expire = get_primary(chunk_handle.get_uid())
          chunk_handle.lease_time = time_to_expire
          chunk_handle.primary = primary

          if time_to_expire < min_lease_time:
            min_lease_time = time_to_expire
            chunk_with_min_lease_time = chunk_handle

      if chunk_with_min_lease_time is None:
        time.sleep(LEASE_TIME/2)
      elif is_alive(chunk_with_min_lease_time.primary):
        time_to_set = min(min_lease_time, time.perf_counter() + LEASE_TIME)
        set_lease(chunk_with_min_lease_time, time_to_set)
        chunk_with_min_lease_time.lease_time = time_to_set


      min_lease_time = float('inf')

      for file_to_chunk_dict in self.file_to_chunk_handles.values():
        for chunk_handle in file_to_chunk_dict.values():
          if chunk_handle.lease_time < min_lease_time:
            min_lease_time = chunk_handle.lease_time

      if min_lease_time - time.perf_counter() > 0.1:
        time.sleep((min_lease_time - time.perf_counter())/2)



  def __enter__(self):
    self.start()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.stop()

  def __del__(self):
    self.stop()

  def start(self) -> None:
    chunk_handle_thread = Thread(target=self.listen_for_chunk_requests, args=(self.chunk_handle_event,))
    chunk_handle_thread.start()

    lease_update_thread = Thread(target=self.handle_lease, args=(self.lease_update_event,))

    lease_update_thread.start()

    # Can't do join as it will block the server

  def stop(self) -> None:
    self.chunk_handle_event.set()
    self.lease_update_event.set()
