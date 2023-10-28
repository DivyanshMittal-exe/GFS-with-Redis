from typing import List

from GFS import config
from redis.client import Redis

from GFS.chunk import ChunkHandle

PRIMARY_KEY = 'primary'
TIME_TO_EXPIRE_KEY = 'time_to_expire'

rds = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, password='', db=0, decode_responses=True)

class Heart:
  def __init__(self, key: str):
    self.key: str = key
  def beat(self, beat_for: int = 2) -> None:
    rds.set(self.key, b'Lub-Dub', ex=beat_for)

  def alive(self):
    return rds.ttl(self.key)



def set_primary(chunk_handel: ChunkHandle) -> None:
  uuid = chunk_handel.get_uid()
  primary = chunk_handel.primary
  lease_time = chunk_handel.lease_time
  servers = chunk_handel.servers

  rds.hset(PRIMARY_KEY,uuid, primary)
  rds.hset(TIME_TO_EXPIRE_KEY, uuid, lease_time)
  servers_key = f'servers:{uuid}'
  rds.delete(servers_key)
  rds.lpush(servers_key, *servers)


def get_primary(uuid: str) -> tuple[str, float]:
  primary = rds.hget(PRIMARY_KEY, uuid)
  primary = primary.decode()
  time_to_expire = rds.hget(TIME_TO_EXPIRE_KEY, uuid)
  time_to_expire = float(time_to_expire.decode())

  return primary, time_to_expire

def get_servers(uuid: str) -> List[str]:
  servers_key = f'servers:{uuid}'
  return rds.lrange(servers_key, 0, -1)