# Design Decisions

# Implementation Overview

# Simplifications

- Access control information not stored. Files are accessible to all for now.
- Worker-Chunk data is persisitent for now. Will store this in Redis
- Using RABBIT-MQ to exchange chunks
- Requesting chunk handle from master is blocking. It will publish a message and wait for exactly 1 message
- In case I have an old chunk, I will ask others for a chunk

# Features

- If the worker comes back alive before the lease expires, we will always renew its lease. Version number is never
updated un-necessarily