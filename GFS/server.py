from collections import defaultdict
import pika
from threading import Event, Thread

from retry import retry

from GFS.chunk import ChunkHandle
from GFS.config import PIKA_CONNECTION_PARAMETERS, SERVER_REPLY_EXCHANGE, SERVER_REPLY_QUEUE, SERVER_REQUEST_EXCHANGE, SERVER_REQUEST_QUEUE, get_filename_and_offset


class GFS_Server:
    
    def __init__(self) -> None:
        self.file_to_chunk_handles = defaultdict(dict)
        self.chunk_handle_to_metadata = defaultdict(ChunkHandle)
        self.chunk_handle_event = Event()



    def listen_for_chunk_requests(self, event: Event) -> None:
        connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
        channel = connection.channel()
        request_queue = channel.queue_declare(queue=SERVER_REQUEST_QUEUE, exclusive=False)
        channel.queue_declare(queue=SERVER_REPLY_QUEUE, exclusive=False)
        # channel.exchange_declare(exchange=SERVER_REQUEST_EXCHANGE)
        # channel.exchange_declare(exchange=SERVER_REPLY_EXCHANGE)

        while True:
            for method_frame, properties, body in channel.consume(queue=request_queue.method.queue,
                                                                       auto_ack=True,
                                                                       inactivity_timeout=2):

                if method_frame is None:
                    break

                key = body.decode()

                filename, offset = get_filename_and_offset(key)

                if filename not in self.file_to_chunk_handles or offset not in self.file_to_chunk_handles[filename]:

                    # TODO: Handle when chunk not here
                    pass



                chunk_handle = self.file_to_chunk_handles[filename][offset]

                channel.basic_publish(  exchange=SERVER_REPLY_EXCHANGE,
                                        routing_key=properties.reply_to,
                                        body=chunk_handle,
                                        properties=pika.BasicProperties(headers={'key': key}))

            if event.is_set():
                break


            requeued_messages = channel.cancel()
            print('Requeued %i messages' % requeued_messages)

        channel.close()
        connection.close()


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

        # Can't do join as it will block the server
    
    def stop(self) -> None:
        self.chunk_handle_event.set()
        