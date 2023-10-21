from GFS import config
from redis.client import Redis


rds = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, password='', db=0, decode_responses=True)

class Heart:
  def __init__(self, key: bytes):
    self.key: bytes = key
  def beat(self, beat_for: int = 2) -> None:
    rds.set(self.key, b'Lub-Dub', ex=beat_for)

  def alive(self):
    return rds.ttl(self.key)

