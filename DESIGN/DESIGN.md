# Design Decisions

# Implementation Overview

# Simplifications

- Access control information not stored. Files are accessible to all for now.
- Worker-Chunk data is persisitent for now. Will store this in Redis
- Using RABBIT-MQ to exchange chunks