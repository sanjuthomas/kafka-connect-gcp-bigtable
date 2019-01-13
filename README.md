# Kafka Sink Connect GCP Bigtable

Apache Kafka Sink only Connect to stream messages from Apache Kafka to Google Cloud Platform (GCP) wide column store Bigtable.

## High Level Architecture

This project use [bigtable-client-core] (https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core) library to stream data to GCP Bigtable. [bigtable-client-core](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core) internally use the [gRPC](https://grpc.io/) protocol to talk to GCP Bigtable.

### add diagram

## Prerequisites
1. Java 8 or greater
2. Apache Zookeeper 
3. Apache Kafka 

## Configurations


