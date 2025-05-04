# Kafka Client

## What is Kafka Client?

Kafka Client is a crate provide a client which can communicate with Kafka Cluster base on Cooperative Thread-per-Core `async`/`await` architect. Kafka Client was built on top of `glommio` and `kafka_protocol`.

## Current supported requests types

- Produce
- Metadata
- ApiVersions

## TODO list

- Test the throughput of producing record concurrently.
