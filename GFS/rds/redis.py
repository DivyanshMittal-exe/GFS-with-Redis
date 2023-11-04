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


def is_alive(key):
  return rds.ttl(key) >= -1


## Only call this during chunk creation
def set_primary_during_chunk_creation(chunk_handel: ChunkHandle) -> None:
  uuid = chunk_handel.get_uid()
  primary = chunk_handel.primary
  lease_time = chunk_handel.lease_time
  servers = chunk_handel.servers

  rds.hset(PRIMARY_KEY,uuid, primary)
  ## Note in server this value is changed when worker is alive
  rds.hset(TIME_TO_EXPIRE_KEY, uuid, lease_time)

  chunk_handel_key = f'chunk_handle:{uuid}'
  rds.hset(chunk_handel_key, 'MASTER', chunk_handel.version)

  for server in servers:
    rds.hset(chunk_handel_key, server, chunk_handel.version)

  servers_key = f'servers:{uuid}'
  rds.delete(servers_key)
  rds.lpush(servers_key, *servers)

def set_lease(chunk_handle: ChunkHandle, time: float)->None:
  rds.hset(TIME_TO_EXPIRE_KEY, chunk_handle.get_uid(), time)


def get_primary(uuid: str) -> tuple[str, float]:
  primary = rds.hget(PRIMARY_KEY, uuid)
  print(f"The primary of {uuid} is {primary}")
  time_to_expire = rds.hget(TIME_TO_EXPIRE_KEY, uuid)
  time_to_expire = float(time_to_expire)

  return primary, time_to_expire

def get_version(uuid: str)-> int:
  version_key = f'chunk_handle:{uuid}'
  latest_version = rds.hget(version_key, 'MASTER')
  return int(latest_version)



def get_servers(uuid: str) -> List[str]:
  servers_key = f'servers:{uuid}'
  return rds.lrange(servers_key, 0, -1)