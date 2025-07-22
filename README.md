[![codecov](https://codecov.io/gh/sanjuthomas/kafka-connect-gcp-bigtable/branch/master/graph/badge.svg)](https://codecov.io/gh/sanjuthomas/kafka-connect-gcp-bigtable)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ce37564e2e4842ae8b08038f53a5be05)](https://www.codacy.com/manual/sanjuthomas/kafka-connect-gcp-bigtable?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=sanjuthomas/kafka-connect-gcp-bigtable&amp;utm_campaign=Badge_Grade)
[![Maintainability](https://api.codeclimate.com/v1/badges/a1ebe21fb92d3e38d599/maintainability)](https://codeclimate.com/github/sanjuthomas/kafka-connect-gcp-bigtable/maintainability)
[![codebeat badge](https://codebeat.co/badges/5f9a8323-7e30-48e4-8fee-c1ae4fd88331)](https://codebeat.co/projects/github-com-sanjuthomas-kafka-connect-gcp-bigtable-master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.sanjuthomas/kafka-connect-gcp-bigtable/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.sanjuthomas/kafka-connect-gcp-bigtable)
[![BCH compliance](https://bettercodehub.com/edge/badge/sanjuthomas/kafka-connect-gcp-bigtable?branch=master)](https://bettercodehub.com/)

# Kafka Sink Connect Google Cloud (GCP) Bigtable

Apache Kafka Sink Only Connect can stream messages from Apache Kafka to Google Cloud Platform (GCP) wide column store Bigtable.

## What is Apache Kafka

Apache Kafka is an open-source stream processing platform developed by the Apache Software Foundation and written in Scala and Java. The project aims to provide a unified, high-throughput, low-latency platform for real-time data feeds. Please refer to the [Apache Kafka home page](https://kafka.apache.org/).

## What is Google Cloud Bigtable

Bigtable is a compressed, high-performance, proprietary data storage system built on Google File System, Chubby Lock Service, SSTable, and a few other Google technologies. On May 6, 2015, a public version of Bigtable was made available as a service in the Google Cloud Platform. For more details, please take a look at [GCP Bigtable home page](https://cloud.google.com/bigtable/).

## High Level Architecture

This project leverages [bigtable-client-core](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core) library (NO HBase) to stream data to GCP Bigtable. [bigtable-client-core](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core) internally use the [gRPC](https://grpc.io/) framework to talk to GCP Bigtable.

![Kafka Connect GCP Bigtable](kafka-connect-bigtable.png)

## Prerequisites

You have [Apache ZooKeeper](https://zookeeper.apache.org) and [Apache Kafka](https://kafka.apache.org) installed and running on your computer. Please refer to the respective sites to download and start ZooKeeper and Kafka. You also need Java version 11 or above.

### Tested Software Versions

| Software      | Version       |  Note                                 |         
| ------------- |---------------| ------------------------------------- |
| Java          | 11            | Tested using Java 11. |
| Kafka         | 3.3.1         | Please [refer](https://kafka.apache.org/downloads). Tested using kafka_2.13-3.3.1, should work with older versions. | 
| bigtable-client-core | 1.27.1 | Please [refer](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core/1.27.1). |
| Kafka connect-api | 3.3.1     | Please [refer](https://mvnrepository.com/artifact/org.apache.kafka/connect-api/3.3.1). |
| grpc-netty-shaded | 1.51.0    | Please [refer](https://mvnrepository.com/artifact/io.grpc/grpc-netty-shaded/1.51.0). |

## Configurations

Please refer to project [Wiki](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/wiki/Kafka-Connect-GCP-Bigtable-sink-configurations)
						 	 
### Constraints

The current configuration system supports streaming messages from a given topic to a table. You can subscribe to any number of topics, but a topic can be pointed to one and only one table. Say, for example, if you subscribed from a topic named demo-topic, you should have a yml file named demo-topic.yml. That YML file contains all the configuration required to transform and write data into Bigtable.										

## How to build the artifact

Please refer to project [Wiki](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/wiki/How-to-build-the-Kafka-Connect-GCP-Bigtable%3F)

## How to deploy the connector

Please refer to project [Wiki](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/wiki/How-to-deploy-the-Kafka-Connect-GCP-Bigtable-and-verify-the-deployment%3F)

## How to start the connector in stand-alone mode

Please refer to project [Wiki](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/wiki/How-to-start-the-Kafka-Sink-Connect-GCP-Bigtable%3F)

## Questions

Either create issues in this project or send it to bt@sanju.org. Thanks!

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable?ref=badge_large)
