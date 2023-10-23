import time
import uuid
from typing import List, Optional

from pydantic import BaseModel, Field


class ChunkHandle(BaseModel):
  servers: List[str]
  version: int = 0  # Default version as 0
  primary: str
  lease_time : int  # Lease expiration in milliseconds
  chunk_uid: Optional[uuid.UUID] = Field(default=None, alias='chunk_uid')
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    if self.chunk_uid is None:
      self.chunk_uid = uuid.uuid4()

    self.lease_time += time.perf_counter()
