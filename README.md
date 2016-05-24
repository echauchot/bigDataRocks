# Big data rocks !
This repo contains examples to help people using big data technologies such as Spark, Cassandra and Elasticsearch.

## Content
For now it contains a project that loads in parallel (using Spark) a collection of persons into Cassandra and Elasticsearch
and provides also tools to manipulate them:
 - Pipeline and services to load data using local (in JVM) spark cluster
 - DAOs for CRUD operations against Cassandra and Elasticsearch
 - Unit tests that use embedded Cassandra and embedded Elasticsearch
 - Integration tests that run against official Cassandra and Elasticsearch docker containers (see https://github.com/etienne-chauchot/bigDataRocksCI for containers management)

The project is designed to work on a single machine. It contains no main and should be run using the integration tests provided.

## Requirements
 - java 8 and maven (obviously :) )
 - docker for backend containers used by integration tests (see https://github.com/etienne-chauchot/bigDataRocksCI)

## Next steps
 - Add more comprehensive Spark example with map/reduce operations and good practices (for instance, avoid groupby and prefer mapTopair + reduceByKey to avoid shuffle ...)
 - Add examples on how to use Spark SQL
 - Add examples on how to use Spark Streaming





