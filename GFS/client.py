

import pika
from GFS.config import PIKA_CONNECTION_PARAMETERS, SERVER_REPLY_EXCHANGE, SERVER_REPLY_QUEUE, SERVER_REQUEST_EXCHANGE, SERVER_REQUEST_QUEUE, get_key


class GFSClient:
    def __init__(self) -> None:
        self.file_offset_to_chunk_handle = {}
        self.connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
        self.channel = self.connection.channel()
        self.reply_queue = self.channel.queue_declare(queue=SERVER_REPLY_QUEUE, exclusive=False)
        self.channel.queue_declare(queue=SERVER_REQUEST_QUEUE, exclusive=False)
        # self.channel.exchange_declare(exchange=SERVER_REQUEST_EXCHANGE)
        # self.channel.exchange_declare(exchange=SERVER_REPLY_EXCHANGE)

    
    def place_chunk_in_memory(self, ch, method, properties, body):
        header = properties.headers
        key = header['key']
        self.file_offset_to_chunk_handle[key] = body
        self.channel.stop_consuming()
    
    def get_chunk_handle(self, filename, offset) -> str:
        message = f'{filename}.{offset}'
        self.channel.basic_publish( exchange=SERVER_REQUEST_EXCHANGE,
                                    routing_key=SERVER_REQUEST_QUEUE, 
                                    body=message.encode('utf-8'),
                                    properties=pika.BasicProperties(reply_to=self.reply_queue.method.queue))
        
        self.channel.basic_consume(queue=self.reply_queue.method.queue,
                                   on_message_callback=self.place_chunk_in_memory,
                                   auto_ack=True)

        self.channel.start_consuming()



    # def __del__(self):
    #   self.channel.close()
    #   self.connection.close()

    def read(self, filename, offset) -> str:
        key = get_key(filename, offset)
        if key not in self.file_offset_to_chunk_handle:
            self.file_offset_to_chunk_handle[key] = self.get_chunk_handle(filename, offset)
        
         
        