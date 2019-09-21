[![Build Status](https://travis-ci.com/sanjuthomas/kafka-connect-gcp-bigtable.svg?branch=master)](https://travis-ci.com/sanjuthomas/kafka-connect-gcp-bigtable)
[![codecov](https://codecov.io/gh/sanjuthomas/kafka-connect-gcp-bigtable/branch/master/graph/badge.svg)](https://codecov.io/gh/sanjuthomas/kafka-connect-gcp-bigtable)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable?ref=badge_shield)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ce37564e2e4842ae8b08038f53a5be05)](https://www.codacy.com/manual/sanjuthomas/kafka-connect-gcp-bigtable?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=sanjuthomas/kafka-connect-gcp-bigtable&amp;utm_campaign=Badge_Grade)

# Kafka Sink Connect GCP Bigtable

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
| Java          | 1.8.0_161     | You may use java 8 or above. |
| Kafka         | 2.11-2.1.0    | Please [refer](https://kafka.apache.org/downloads) | 
| Zookeeper     | 3.4.13        | Please [refer](https://zookeeper.apache.org/releases.html)         |
| bigtable-client-core | 1.8.0  | Please [refer](https://mvnrepository.com/artifact/com.google.cloud.bigtable/bigtable-client-core/1.12.1)                 |
| Kafka connect-api | 2.1.0     | Please [refer](https://mvnrepository.com/artifact/org.apache.kafka/connect-api/2.3.0) |
| grpc-netty-shaded | 1.17.1    | Please [refer](https://mvnrepository.com/artifact/io.grpc/grpc-netty-shaded/1.23.0) |

## Configurations

### bigtable-sink.properties

| Property      				  | Value       			    | Data Type    | Description     							|   
|---------------------------------|-----------------------------|--------------|------------------------------------------------|
| name          				  | bigtable-sink    		 	| String	   | Name of the Sink Connect.          |     
| connector.class        		  | BigtableSinkConnector       | String	   | Simple name of the Connector Class. |  
| tasks.max        				  | 1 							| Number 	   | Numbers of tasks.				|
| topics						  | demo-topic					| String	   | Comma separated list of topics. 	|
| config.files.location           | kafka_home/config    		| String	   | There should be one yml file per topic.  	|	

### demo-topic.yml (one yml file per topic)

| Property      					| Value  			| Data Type |					   Description       				   		  | 
|-------------------------------|--------------------|--------------|------------------------------------------------------------------|
| keyFile:	   				    |	 /home/keys/demo-instance-key.json | String	 | GCP Connect Key File. This is a topic level configuration because you could subscribe from multiple topics and messages from one topic may go to a table in instance A and messages from another topic may go to a table in instance B |		
| project: 					    | demo-project	| String |					    Name of the GCP Project | 
| instance: 					    | demo-instance	 | String |				    Name of GCP Bigtable instance | 
| table: 							| demo-table | 	 String |				     Name of GCP Bigtable table | 
| transformer: 					| com.sanjuthomas.gcp.transform.JsonEventTransformer | String |	  Transformer class to transform the message to Bigtable writable row. You may provide your own implementation. | 
| keyQualifiers: | 		 - exchange	<br/> - symbol| Array| Bigtable row key qualifier. Configured element names would be used to construct the row keys. | 
| keyDelimiter: | - | String | Delimiter to use if there are more than one element to construct row key. |
| families:  	| - data	 <br/> - metadata | Array | Column families in the Bigtable table. This configuration will be used by the transformer. | 
| familyQualifiers: | - data:	 <br> &nbsp;- client <br> &nbsp;- exchange <br> &nbsp;- symbol <br> &nbsp;- price <br> &nbsp;- quantity	 <br/> - metadata:	 <br> &nbsp;- created_at <br> &nbsp;- processed_at <br> &nbsp;- topic <br> &nbsp;- partition | Array | Column family to columns mapping. | 
						 	 
### Constraints

The current configuration system supports streaming messages from a given topic to a given table. You can subscribe any number of topics, but a topic can be pointed to one and only table. Say for example, if you subscribed from topic named demo-topic, you should have yml file named demo-topic.yml. That yml file contains all the configuration requires to transform and write data into Bigtable.										

As of today, we have transformer support for JSON Messages. I'm planning to add the Avro Messages transformer in the next version.

## How to deploy the connector

This is maven project. To create an [uber](https://maven.apache.org/plugins/maven-shade-plugin/index.html) jar, execute the following maven goals.

```mvn clean compile package shade:shade install```

Copy the artifact ```kafka-connect-gcp-bigtable-1.0.0.jar``` to kakfa_home/lib folder.

Copy the [bigtable-sink.properties](https://github.com/sanjuthomas/kafka-connect-gcp-bigtable/blob/master/src/main/resources/configs/bigtable-sink.properties) file into kafka_home/config folder. Update the content of the property file according to your environment.

Alternatively, you may keep the ```kafka-connect-gcp-bigtable-1.0.jar``` in another directory and export that directory into Kafka class path before starting the connector.

## How to start connector in stand-alone mode

Open a shell prompt, move to kafka_home and execute the following.

```
bin/connect-standalone.sh config/connect-bigtable-standalone.properties config/bigtable-sink.properties
```

## How to start connector in distribute mode

Open a shell prompt, change your working directory to kafka_home and execute the following.

```
bin/connect-distributed.sh config/connect-bigtable-distributed.properties config/bigtable-sink.properties
```

## Questions

Either create an issues in this project or send it to bt@sanju.org. Thanks!

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-gcp-bigtable?ref=badge_large)
