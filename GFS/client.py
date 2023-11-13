import pickle
import random
import uuid
from typing import Tuple, Union

import pika

from GFS.chunk import ChunkHandle
from GFS.config import (
    PIKA_CONNECTION_PARAMETERS,
    SERVER_REPLY_QUEUE,
    SERVER_REQUEST_EXCHANGE,
    SERVER_REQUEST_QUEUE,
    GFSEvent,
    get_key,
    CHUNK_EXCHANGE,
    TIMEOUT,
    StatusCodes,
)


class GFSClient:
    def __init__(self) -> None:
        self.file_offset_to_chunk_handle = {}
        self.chunk_handle_to_data = {}

        self.connection = pika.BlockingConnection(PIKA_CONNECTION_PARAMETERS)
        self.channel = self.connection.channel()
        self.channel2 = self.connection.channel()
        self.reply_queue = self.channel.queue_declare(
            queue=SERVER_REPLY_QUEUE, exclusive=False
        )
        self.channel.queue_declare(queue=SERVER_REQUEST_QUEUE, exclusive=False)

        self.chunk_handle_queue = self.channel.queue_declare(queue="", exclusive=False)

        self.last_write_success = False  # self.channel.exchange_declare(exchange=SERVER_REQUEST_EXCHANGE)  # self.channel.exchange_declare(exchange=SERVER_REPLY_EXCHANGE)

    def get_chunk_handle(self, filename, offset):
        message = get_key(filename, offset)
        self.channel2.basic_publish(
            exchange=SERVER_REQUEST_EXCHANGE,
            routing_key=SERVER_REQUEST_QUEUE,
            body=message.encode("utf-8"),
            properties=pika.BasicProperties(
                reply_to=self.chunk_handle_queue.method.queue
            ),
        )

        print("Published")

        for method_frame, properties, body in self.channel2.consume(
            queue=self.chunk_handle_queue.method.queue,
            auto_ack=True,
            inactivity_timeout=TIMEOUT,
        ):
            if properties is None:
                break

            header = properties.headers
            key = header["key"]

            if header["status"] == StatusCodes.CHUNK_HANDLE_REQUEST_SUCCESSFUL:
                chunk_handle = pickle.loads(body)
                self.file_offset_to_chunk_handle[key] = chunk_handle

            print(key, message, header)

            # os.exit()
            if key == message:
                additional_params = {}
                if header["status"] == StatusCodes.BAD_OFFSET:
                    additional_params["offset"] = header["offset"]

                return header["status"], additional_params

        return StatusCodes.CHUNK_HANDLE_REQUEST_FAIL, {}

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
                headers={
                    "key": str(key_for_the_data),
                    "type": GFSEvent.PUT_DATA_OF_A_CHUNK,
                }
            ),
        )

        write_request_to_server = chunk_handle.primary

        routing_key = f".{write_request_to_server}."

        request_id = str(uuid.uuid4())

        self.channel.basic_publish(
            exchange=CHUNK_EXCHANGE,
            routing_key=routing_key,
            body=b"",
            properties=pika.BasicProperties(
                headers={
                    "chunk_key": str(chunk_handle.chunk_uid),
                    "data_key": str(key_for_the_data),
                    "request_id": str(request_id),
                    "type": GFSEvent.WRITE_TO_CHUNK,
                },
                reply_to=self.reply_queue.method.queue,
            ),
        )

        for method_frame, properties, body in self.channel.consume(
            queue=self.reply_queue.method.queue,
            auto_ack=True,
            inactivity_timeout=TIMEOUT,
        ):
            if method_frame is None:
                break

            header = properties.headers
            status = header["status"]
            type_of_message = header["type"]

            if type_of_message == GFSEvent.ACK_T0_CHUNK_WRITE:
                request_id_returned = header["request_id"]

                print(status)
                print(type_of_message)
                print(request_id_returned)
                print(request_id)

                print(
                    f"request_id_returned: {type(request_id_returned)} == {type(request_id)}, {request_id_returned == request_id} | {type_of_message} | {type_of_message == GFSEvent.ACK_T0_CHUNK_WRITE}"
                )

                if request_id_returned == request_id:
                    return status

        return StatusCodes.WRITE_FAILED
      
      
      

    def read(
        self, filename, offset
    ) -> Tuple[bool, Union[bytes, str], dict]:
      
      
        key = get_key(filename, offset)
        if key not in self.file_offset_to_chunk_handle:
            alpha = self.get_chunk_handle(filename, offset)
            print("Value of", alpha)
            status, params = alpha

            if status != StatusCodes.CHUNK_HANDLE_REQUEST_SUCCESSFUL:
                # os.exit()
                print(
                    f"Oh no, I have failed to get the chunk handle for {filename} at {offset}"
                )
                return False, status, params

        chunk_handle: ChunkHandle = self.file_offset_to_chunk_handle[key]
        chunk_handle_uid = str(chunk_handle.chunk_uid)

        request_chunk_from_server = random.choice(chunk_handle.servers)
        routing_key = f".{request_chunk_from_server}."

        self.channel.basic_publish(
            exchange=CHUNK_EXCHANGE,
            routing_key=routing_key,
            body=b"",
            properties=pika.BasicProperties(
                headers={"key": chunk_handle_uid, 
                         "type": GFSEvent.GET_CHUNK,
                         'version': chunk_handle.version
                         },
                reply_to=self.reply_queue.method.queue,
            ),
        )

        for method_frame, properties, body in self.channel.consume(
            queue=self.reply_queue.method.queue,
            auto_ack=True,
            inactivity_timeout=TIMEOUT,
        ):
            if method_frame is None:
                break

            print(f"Received a message {method_frame}, {properties}")

            header = properties.headers
            status = header["status"]
            type = header["type"]
            
            
            if type != GFSEvent.ACK_TO_CHUNK_READ:
                continue

            if status != StatusCodes.READ_SUCCESS:
                del self.file_offset_to_chunk_handle[key]
                return False, b"", {}

            header = properties.headers
            header_key = header["key"]

            if header_key == chunk_handle_uid:
                data = b""
                chunk_data_list = pickle.loads(body)
                for chunk_data in chunk_data_list:
                    if chunk_data is not None:
                        # print(chunk_data[:10])
                        data += chunk_data

                return True, data, {}

        return False, b"", {}
