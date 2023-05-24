# Weather Monitoring System

This project implements a distributed weather monitoring system consisting of 
multiple weather stations that feed a queueing service (Kafka) with their readings. 
The central base station consumes the streamed data and archives all data in the 
form of Parquet files. Two variants of index are maintained: a key-value store 
(Bitcask) for the latest reading from each individual station and 
ElasticSearch/Kibana that are running over the Parquet files.

## System Architecture

The system is composed of three stages:

1. Data Acquisition: Multiple weather stations that feed a queueing service (Kafka) with their readings
2. Data Processing & Archiving: The base central station is consuming the streamed data and archiving all data in the form of Parquet files
3. Indexing: Two variants of index are maintained:
   - Key-value store (Bitcask) for the latest reading from each individual station
   - ElasticSearch/Kibana that are running over the Parquet files

## Installation

For the purpose of this lab, we will use a Kubernetes application containing a cluster of:

- 10 weather stations
- 1 Kafka service + 1 Zookeeper service (for Kafka)
- 1 Elastic + Kibana service
- 1 central base station service

To set up this cluster, you need first to:

- Install Docker
- Pull Bitnami Kafka Docker Image (Includes zookeeper)
- Pull image containing ElasticSearch and Kibana## Implementation

### A) Write Weather Station Mock

Each weather station should output a status message every 1 second to report its sampled weather status. To simulate data with query-able nature, you will have to:

- Randomly change the battery_status field by the following specs:
  - Low = 30% of messages per service
  - Medium = 40% of messages per service
  - High = 30% of messages per service
- Randomly drop messages on a 10% rate

Weather Status Message

The weather status message should be of the following schema:

```json
{
  "station_id": 1, // Long
  "s_no": 1, // Long auto-incremental with each message per service
  "battery_status": "low", // String of (low, medium, high)
  "status_timestamp": 1681521224, // Long Unix timestamp
  "weather": {
    "humidity": 35, // Integer percentage
    "temperature": 100, // Integer in fahrenheit
    "wind_speed": 13 // Integer km/h
  }
}
```

### B) Set up Weather Station to connect to Kafka

Use the produce API to send messages to the Kafka server. You should use the Java programmatic API for this task.

### C) Implement Raining Triggers in Kafka Processors

Use Kafka Processors to detect when it's raining. You should use Kafka Processorto detect if humidity is higher than 70%. The processor should output a special message to a specific topic.

### D) Implement Central Station

Implement BitCask Riak to store an updated view of weather status. You should maintain a key-value store of the station's statuses. To do this efficiently, you are going to implement the BitCask Riak LSM to maintain an updated store of each station status.

The implementation should be as discussed in lectures with some notes:

- You are required to implement hint files to help in rehash for recovery (will be tested)
- Compaction should be scheduled to run over replicas of the segment files to avoid disrupting active readers
- You are NOT required to implement checksums to detect errors
- You are NOT required to implement tombstones for deletions as there is no point in deleting some weather station ID entry for sport.

Implement Archiving of all Weather Statuses

Aside from maintaining an updated LSM for updated statuses, you should archive all weather statuses history for all stations.

We can append all weather statuses into Parquet files. For this purpose, your central server should write all received statuses to Parquet files and partition them by time and station ID.

You should write records in batches to the Parquet to avoid blocking on IO frequently. Common batch size could be 10K records.

### E) Set up Historical Weather Statuses Analysis

You should direct all weather statuses to ElasticSearch for indexing and querying by Kibana.

The usecases for such analysis are:

- Count of low-battery statuses per station (should confirm percentages above)
- Count of dropped messages per station (should confirm percentages above)

In order to do that, connect Parquet files as a data source for ElasticSearch so you can index using ElasticSearch and visualize using Kibana.

### F) Deploy using Kubernetes

You are required to deploy all of this using Docker. In order to do that, you need to do a few steps:

- Write Dockerfile for the central server
- Write Dockerfile for the weather stations
- Write K8s YAML file to incorporate:
  - 10 services with an image for the weather station
  - Service with an image for the central server you wrote.
  - 2 services with Kafka & Zookeeper images
  - Shared storage for writing Parquet files and BitCask Riak LSM

### G) Profile Central Station using JFR

Since the Central Station is the heart of this Data-Intensive Application, it is of the utmost importance that we make sure that our code is free of any unnecessary overheads. One way to do this is to use a profiling tool to measure execution time, memory consumption, GC pauses, and their durations, etc.

Java Flight Recorder (JFR) is a tool for collecting diagnostic and profiling data about a running Java application. It is integrated into the Java Virtual Machine (JVM) and causes almost no performance overhead, so it can be usedeven in heavily loaded production environments.

You are required to profile your central station using JFR (Java Flight Recorder). You should run the system for 1 minute and report the following:

- Top 10 Classes with the highest total memory
- GC pauses count
- GC maximum pause duration
- List of I/O operations

## Getting Started

To get started, follow the installation instructions above to set up the Kubernetes application containing the cluster of weather stations, Kafka service, ElasticSearch/Kibana service, and central base station service.

## Usage

After setting up the Kubernetes application, you can start the weather stations and the central base station to start collecting and processing weather data. The system will archive all data in the form of Parquet files and maintain two variants of index: a key-value store (Bitcask) for the latest reading from each individual station and ElasticSearch/Kibana that are running over the Parquet files.

## Credits

This project was created as course project of Data Intensive Applications course. It was implemented using Java, Kafka, ElasticSearch, Kibana, Bitcask Riak, and Parquet.