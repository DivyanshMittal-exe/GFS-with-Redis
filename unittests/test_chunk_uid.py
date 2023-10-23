import unittest

from GFS.chunk import ChunkHandle

class uid_chunk_test(unittest.TestCase):

    def test_chunk_uid(self):
        servers = ["192.168.1.1", "192.168.1.2"]
        primary = "192.168.1.1"
        lease_time = 10
        
        chunk_handle_1 = ChunkHandle(servers=servers, primary=primary, lease_time=lease_time)
        chunk_handle_2 = ChunkHandle(servers=servers, primary=primary, lease_time=lease_time)
        
        self.assertNotEqual(chunk_handle_1.get_uid(), chunk_handle_2.get_uid())
