[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/blob/master/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable?ref=badge_shield)
[![Build Status](https://travis-ci.com/sanjuthomas/kafka-connect-gcp-bigtable.svg?branch=master)](https://travis-ci.com/sanjuthomas/kafka-connect-gcp-bigtable)
[![codecov](https://codecov.io/gh/sanjuthomas/kafka-connect-gcp-bigtable/branch/master/graph/badge.svg)](https://codecov.io/gh/sanjuthomas/kafka-connect-gcp-bigtable)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ce37564e2e4842ae8b08038f53a5be05)](https://www.codacy.com/manual/sanjuthomas/kafka-connect-gcp-bigtable?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=sanjuthomas/kafka-connect-gcp-bigtable&amp;utm_campaign=Badge_Grade)
[![Maintainability](https://api.codeclimate.com/v1/badges/a1ebe21fb92d3e38d599/maintainability)](https://codeclimate.com/github/sanjuthomas/kafka-connect-gcp-bigtable/maintainability)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.sanjuthomas/kafka-connect-gcp-bigtable/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.sanjuthomas/kafka-connect-gcp-bigtable)

# Kafka Sink Connect Google Cloud (GCP) Bigtable

Apache Kafka Sink only Connect can be used to stream messages from Apache Kafka to Google Cloud Platform (GCP) wide column store Bigtable.

## What is Apache Kafka

Apache Kafka is an open-source stream processing platform developed by the Apache Software Foundation written in Scala and Java. The project aims to provide a unified, high-throughput, low-latency platform for handling real-time data feeds. For more details, please refer to [Apache Kafka home page](https://kafka.apache.org/).

## What is Google Cloud Bigtable

Bigtable is a compressed, high performance, proprietary data storage system built on Google File System, Chubby Lock Service, SSTable and a few other Google technologies. On May 6, 2015, a public version of Bigtable was made available as a service in the Google Cloud Platform. For more details, please refer to [GCP Bigtable home page](https://cloud.google.com/bigtable/).

## High Level Architecture

This project leverage [bigtable-client-core](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core) library (NO HBase) to stream data to GCP Bigtable. [bigtable-client-core](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core) internally use the [gRPC](https://grpc.io/) framework to talk to GCP Bigtable.

![Kafka Connect GCP Bigtable](kafka-connect-bigtable.png)

## Prerequisites

[Apache ZooKeeper](https://zookeeper.apache.org) and [Apache Kafka](https://kafka.apache.org) installed and running in your machine. Please refer to respective sites to download and start ZooKeeper and Kafka. You would also need Java version 8 or above.

### Tested Software Versions

| Software      | Version       |  Note                                 |         
| ------------- |---------------| --------------------------------------- |
| Java          | 1.8.0_161     | You may use java 8 or above. Tested using Java 8. |
| Kafka         | >= 2.11-1.1.1    | Please [refer](https://kafka.apache.org/downloads). Tested using kafka_2.11-1.1.1, may work with older versions. | 
| Zookeeper     | >= 3.4.13        | Please [refer](https://zookeeper.apache.org/releases.html). Tested using zookeeper-3.4.13.  |
| bigtable-client-core | 1.12.1  | Please [refer](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core/1.12.1).                 |
| Kafka connect-api | 2.3.0     | Please [refer](https://mvnrepository.com/artifact/org.apache.kafka/connect-api/2.3.0). |
| grpc-netty-shaded | 1.23.0    | Please [refer](https://mvnrepository.com/artifact/io.grpc/grpc-netty-shaded/1.23.0). |

## Configurations

Please refer to project [Wiki](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/wiki/Kafka-Connect-GCP-Bigtable-sink-configurations)
						 	 
### Constraints

The current configuration system supports streaming messages from a given topic to a given table. You can subscribe any number of topics, but a topic can be pointed to one and only table. Say for example, if you subscribed from topic named demo-topic, you should have yml file named demo-topic.yml. That yml file contains all the configuration requires to transform and write data into Bigtable.										

As of today, we have transformer support for JSON Messages. I'm planning to add the Avro Messages transformer in the next version.

## How to build the artifact

Please refer to project [Wiki](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/wiki/How-to-build-the-Kafka-Connect-GCP-Bigtable%3F)

## How to deploy the connector

Please refer to project [Wiki](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/wiki/How-to-deploy-the-Kafka-Connect-GCP-Bigtable-and-verify-the-deployment%3F)

## How to start connector in stand-alone mode

Please refer to project [Wiki](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/wiki/How-to-start-the-Kafka-Sink-Connect-GCP-Bigtable%3F)

## Questions

Either create an issues in this project or send it to bt@sanju.org. Thanks!

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable?ref=badge_large)
