import time
import unittest

from GFS.rds.redis import Heart


class heart_test(unittest.TestCase):

  def test_heart(self):
    heart_name: bytes = b'Nischay'
    my_heart = Heart(heart_name)
    my_heart.beat(1)
    time.sleep(0.5)

    self.assertEqual(my_heart.alive() >= -1, True)
    time.sleep(1)
    self.assertEqual(my_heart.alive() < -1, True)

