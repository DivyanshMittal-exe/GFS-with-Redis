import pickle
import random
import time
from collections import defaultdict
from threading import Event, Thread
from typing import List

import pika

from GFS.chunk import ChunkHandle
from GFS.config import PIKA_CONNECTION_PARAMETERS, SERVER_REPLY_EXCHANGE, SERVER_REPLY_QUEUE, SERVER_REQUEST_QUEUE, \
  get_filename_and_offset, WORKER_COUNT_WITH_CHUNK, LEASE_TIME, StatusCodes, CHUNK_EXCHANGE, GFSEvent
from GFS.rds.redis import set_primary_during_chunk_creation, is_alive, set_lease, get_primary, get_my_version, \
  update_chunk_version


class GFS_Server:

  def __init__(self, worker_names: List[str]) -> None:
    self.file_to_chunk_handles = defaultdict(dict)
    self.chunk_handle_to_metadata = defaultdict(ChunkHandle)
    self.chunk_handle_event = Event()
    self.lease_update_event = Event()
    self.handle_dead_primary_event = Event()
    self.worker_names = worker_names

    self.connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
    self.channel = self.connection.channel()
    self.request_queue = self.channel.queue_declare(queue=SERVER_REQUEST_QUEUE, exclusive=False)
    self.channel.queue_declare(queue=SERVER_REPLY_QUEUE, exclusive=False)
    self.version_number_update_queue = self.channel.queue_declare(queue='', exclusive=True)

  def handle_errors(self, status_code: StatusCodes, **kwargs):
    if status_code == StatusCodes.BAD_OFFSET:
      print(f'Bad offset {kwargs["offset"]}')
      recommended_offset = kwargs['offset']
      key = kwargs['key']
      properties = kwargs['properties']

      self.channel.basic_publish(exchange=SERVER_REPLY_EXCHANGE, routing_key=properties.reply_to, body=b'',
                                 properties=pika.BasicProperties(headers={'key': key, 'status': StatusCodes.BAD_OFFSET,
                                   'offset': recommended_offset}))




    else:
      raise NotImplementedError

  def handle_dead_primary(self, handle_dead_primary_event: Event):

    while not handle_dead_primary_event.is_set():
      time.sleep(LEASE_TIME / 20)

      for file_name, file_to_chunk_dict in self.file_to_chunk_handles.items():
        for offset, chunk_handle in file_to_chunk_dict.items():
          _, time_to_expire = get_primary(chunk_handle.get_uid())

          if time_to_expire < time.perf_counter():
            # raise OSError
            my_version = chunk_handle.version
            new_version = my_version + 1
            servers = chunk_handle.servers
            id = chunk_handle.get_uid()

            candidates = [server for server in servers if get_my_version(server, id) == my_version]

            routing_key = f'.{".".join(candidates)}'

            self.channel.basic_publish(exchange=CHUNK_EXCHANGE, routing_key=routing_key, body=b'',
                                       properties=pika.BasicProperties(
                                         headers={'key': id, 'type': GFSEvent.UPDATE_CHUNK_VERSION},
                                         reply_to=self.version_number_update_queue.method.queue))

            approved_candidate = None
            for method_frame, properties, body in self.channel.consume(
                    queue=self.version_number_update_queue.method.queue, auto_ack=True,
                    inactivity_timeout=LEASE_TIME / 20):
              header = properties.headers
              if header['key'] == id:
                if get_my_version(header['name']) == new_version:
                  approved_candidate = header['name']

            assert approved_candidate is not None, f'Lost the chunk {id}, everyone is dead or old'

            new_primary = approved_candidate
            new_lease = time.perf_counter() + LEASE_TIME

            chunk_handle.version = new_version
            chunk_handle.primary = new_primary
            chunk_handle.lease_time = new_lease

            update_chunk_version(chunk_handle)
            self.file_to_chunk_handles[file_name][offset] = chunk_handle

  def listen_for_chunk_requests(self, event: Event) -> None:

    while True:
      for method_frame, properties, body in self.channel.consume(queue=self.request_queue.method.queue, auto_ack=False,
              inactivity_timeout=5):

        if method_frame is None:
          break

        key = body.decode()

        filename, offset = get_filename_and_offset(key)

        if filename not in self.file_to_chunk_handles or offset not in self.file_to_chunk_handles[filename]:

          if filename not in self.file_to_chunk_handles:
            if offset != 0:
              kwargs = {'offset': 0, 'key': key, 'properties': properties}
              self.handle_errors(StatusCodes.BAD_OFFSET, **kwargs)
              continue

            self.file_to_chunk_handles[filename] = {}
          else:
            if offset - 1 not in self.file_to_chunk_handles[filename]:
              last_offset_in_mem = max(self.file_to_chunk_handles[filename].keys())
              kwargs = {'offset': last_offset_in_mem, 'key': key, 'properties': properties}
              self.handle_errors(StatusCodes.BAD_OFFSET, **kwargs)
              continue

          workers_that_will_have_the_chunk = random.sample(self.worker_names, WORKER_COUNT_WITH_CHUNK)

          chunk_handle = ChunkHandle(servers=workers_that_will_have_the_chunk,
                                     primary=workers_that_will_have_the_chunk[0], lease_time=LEASE_TIME)

          set_primary_during_chunk_creation(chunk_handle)

          self.file_to_chunk_handles[filename][offset] = chunk_handle

        chunk_handle = self.file_to_chunk_handles[filename][offset]
        chunk_handle_serialised = pickle.dumps(chunk_handle)

        self.channel.basic_publish(exchange=SERVER_REPLY_EXCHANGE, routing_key=properties.reply_to,
                                   body=chunk_handle_serialised, properties=pika.BasicProperties(
            headers={'key': key, 'status': StatusCodes.CHUNK_HANDLE_REQUEST_SUCCESSFUL}))

      if event.is_set():
        break

      requeued_messages = self.channel.cancel()
      print('Requeued %i messages' % requeued_messages)

    self.channel.close()
    self.connection.close()

  def handle_lease(self, lease_update_event: Event):

    while not lease_update_event.is_set():
      min_lease_time = float('inf')
      chunk_with_min_lease_time = None

      for file_to_chunk_dict in self.file_to_chunk_handles.values():
        for chunk_handle in file_to_chunk_dict.values():
          primary, time_to_expire = get_primary(chunk_handle.get_uid())
          chunk_handle.lease_time = time_to_expire
          chunk_handle.primary = primary

          if time_to_expire > time.perf_counter():
            if time_to_expire < min_lease_time:
              min_lease_time = time_to_expire
              chunk_with_min_lease_time = chunk_handle

      if chunk_with_min_lease_time is None:
        pass
      elif is_alive(chunk_with_min_lease_time.primary):
        time_to_set = min(min_lease_time, time.perf_counter() + LEASE_TIME)
        set_lease(chunk_with_min_lease_time, time_to_set)
        chunk_with_min_lease_time.lease_time = time_to_set
      else:
        # raise OSError

        print(f'Looks like someone is dead: {chunk_with_min_lease_time}|{chunk_with_min_lease_time.primary}')

      min_lease_time = float('inf')

      for file_to_chunk_dict in self.file_to_chunk_handles.values():
        for chunk_handle in file_to_chunk_dict.values():
          if chunk_handle.lease_time > time.perf_counter():
            if chunk_handle.lease_time < min_lease_time:
              min_lease_time = chunk_handle.lease_time

      # CLIP the sleep time between LEASE_TIME/20 and LEASE_TIME/2
      sleep_time = min(max(((min_lease_time - time.perf_counter()) / 2), LEASE_TIME / 20), LEASE_TIME / 2)
      time.sleep(sleep_time)

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

    handle_dead_primary_thread = Thread(target=self.handle_dead_primary, args=(self.handle_dead_primary_event,))
    handle_dead_primary_thread.start()

    # Can't do join as it will block the server

  def stop(self) -> None:
    self.chunk_handle_event.set()
    self.lease_update_event.set()
    self.handle_dead_primary_event.set()
