import pickle
import random
import uuid

import pika
from GFS.config import PIKA_CONNECTION_PARAMETERS, SERVER_REPLY_EXCHANGE, SERVER_REPLY_QUEUE, SERVER_REQUEST_EXCHANGE, \
    SERVER_REQUEST_QUEUE, GFSEvent, get_key, CHUNK_EXCHANGE
from GFS.chunk import ChunkHandle

class GFSClient:
    def __init__(self) -> None:
        self.file_offset_to_chunk_handle = {}
        self.chunk_handle_to_data = {}

        self.connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
        self.channel = self.connection.channel()
        self.reply_queue = self.channel.queue_declare(queue=SERVER_REPLY_QUEUE, exclusive=False)
        self.channel.queue_declare(queue=SERVER_REQUEST_QUEUE, exclusive=False)

        self.last_write_success = False
        # self.channel.exchange_declare(exchange=SERVER_REQUEST_EXCHANGE)
        # self.channel.exchange_declare(exchange=SERVER_REPLY_EXCHANGE)

    
    def place_chunk_handle_in_memory(self, ch, method, properties, body):
        header = properties.headers
        key = header['key']
        chunk_handle = pickle.loads(body)
        self.file_offset_to_chunk_handle[key] = chunk_handle
        self.channel.stop_consuming()
    
    def get_chunk_handle(self, filename, offset) -> None:
        message = get_key(filename,offset)
        self.channel.basic_publish( exchange=SERVER_REQUEST_EXCHANGE,
                                    routing_key=SERVER_REQUEST_QUEUE, 
                                    body=message.encode('utf-8'),
                                    properties=pika.BasicProperties(reply_to=self.reply_queue.method.queue))
        
        self.channel.basic_consume(queue=self.reply_queue.method.queue,
                                   on_message_callback=self.place_chunk_handle_in_memory,
                                   auto_ack=True)

        self.channel.start_consuming()



    def read_chunk_from_pika(self, ch, method, properties, body):
        header = properties.headers
        key = header['key']
        self.chunk_handle_to_data[key] = body
        self.channel.stop_consuming()

    def write_chunk_from_pika(self, ch, method, properties, body):
        header = properties.headers
        print(header)
        key = header['status']
        if key == 'True':
          self.last_write_success = True
        self.channel.stop_consuming()

    def write(self, filename, offset, data) -> bool:

      key = get_key(filename, offset)
      if key not in self.file_offset_to_chunk_handle:
        self.get_chunk_handle(filename, offset)

      chunk_handle: ChunkHandle = self.file_offset_to_chunk_handle[key]

      workers_with_this_chunk = chunk_handle.servers
      routing_key_to_place_chunk = f".{'.'.join(workers_with_this_chunk)}."

      key_for_the_data = uuid.uuid4()

      self.channel.basic_publish(
          exchange=CHUNK_EXCHANGE,
          routing_key=routing_key_to_place_chunk,
          body=data,
          properties=pika.BasicProperties(
            headers={'key': str(key_for_the_data),
                     'type': GFSEvent.PUT_CHUNK})
      )

      write_request_to_server = chunk_handle.primary

      routing_key = f'.{write_request_to_server}.'


      request_id = uuid.uuid4()

      self.channel.basic_publish(
          exchange=CHUNK_EXCHANGE,
          routing_key=routing_key,
          body=b'',
          properties=pika.BasicProperties(
              headers={'chunk_key': str(chunk_handle.chunk_uid),
                       'data_key': str(key_for_the_data),
                       'request_id': str(request_id),
                       'type': GFSEvent.WRITE_TO_CHUNK},
              reply_to=self.reply_queue.method.queue
          )
      )

      self.channel.basic_consume(queue=self.reply_queue.method.queue,
                                 on_message_callback=self.write_chunk_from_pika,
                                 auto_ack=True)

      self.channel.start_consuming()

      if self.last_write_success:
        self.last_write_success = False
        return True

      return False

    def read(self, filename, offset) -> bytes:
        key = get_key(filename, offset)
        if key not in self.file_offset_to_chunk_handle:
            self.get_chunk_handle(filename, offset)

        chunk_handle: ChunkHandle = self.file_offset_to_chunk_handle[key]
        chunk_handle_uid = str(chunk_handle.chunk_uid)

        request_chunk_from_server = random.choice(chunk_handle.servers)
        routing_key = f'.{request_chunk_from_server}.'

        self.channel.basic_publish(
                exchange=CHUNK_EXCHANGE,
                routing_key=routing_key,
                body = '',
                properties=pika.BasicProperties(
                        headers={'key':chunk_handle_uid, 'type': GFSEvent.GET_CHUNK},
                        reply_to=self.reply_queue.method.queue
                    )
            )

        self.channel.basic_consume(queue=self.reply_queue.method.queue,
                                   on_message_callback=self.read_chunk_from_pika,
                                   auto_ack=True)

        self.channel.start_consuming()
        # TODO: Delete from dictionary before returning
        return self.chunk_handle_to_data[chunk_handle_uid]



        
         
        