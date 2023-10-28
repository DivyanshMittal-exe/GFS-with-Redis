import unittest
import time

from GFS.chunk import ChunkHandle
from GFS.client import GFSClient
from GFS.config import get_key
from GFS.server import GFS_Server


class chunk_handle_exchange_test(unittest.TestCase):

    def test_chunk_handle_exchange(self):
        
        client = GFSClient()
        with GFS_Server(['']) as server:
            servers = ["192.168.1.1", "192.168.1.2"]
            primary = "192.168.1.1"
            lease_time = 10

            chunk_handle_1 = ChunkHandle(servers=servers, primary=primary, lease_time=lease_time)
            chunk_handle_2 = ChunkHandle(servers=servers, primary=primary, lease_time=lease_time)

            server.file_to_chunk_handles["abc"][0] = chunk_handle_1
            server.file_to_chunk_handles["abc"][1] = chunk_handle_2
            
            client.get_chunk_handle("abc", 0)
            client.get_chunk_handle("abc", 1)
            
            time.sleep(1)
            
            key_1 = get_key("abc", 0)
            key_2 = get_key("abc", 1)


            self.assertEqual(client.file_offset_to_chunk_handle[key_1].chunk_uid, chunk_handle_1.chunk_uid)
            self.assertEqual(client.file_offset_to_chunk_handle[key_2].chunk_uid, chunk_handle_2.chunk_uid)
            
            
        
        
        