# dspbench-storm

This is the implementation of the DSPBench stream processing benchmark for Apache Storm.

## Project Information

This project is developed with Java 11 and uses Gradle as the build tool. All build configuration and dependencies are on `build.gralde`.
The project can be built directly with `gradlew` or using Docker for testing purposes. We have also a `Makefile` to help running some basic commands:

 - `make build-native`: to build the project and generate a fat JAR with all project dependencies (ready for deployment)
 - `make build-docker`: to build a Docker image with Java and the fat JAR to run Storm as a standalone application.
 - `make run-docker APP=<app-name>`: to run the built Docker image with the application defined as argument.

### Code Structure

 - `applications`: this package contains subfolders, one per application, and within each application folder we have all the code of the application (except for maybe a few utilities that are more generic).
 - `bolt`: contains the abstract bolt class that all bolts must extend.
 - `constants`: contains the base constants interface with configuration keys that can be used for all applications. All configuration keys start with a mask because this mask will be replace by the application prefix. For example the key `%s.spout.threads` will be `wc.spout.threads` for the `wordcount` application.
 - `hooks`: hooks are a functionality provided by Storm to run small pieces of code before and after a tuple is executed within a spout or bolt. This can be used to gather metrics about the application.
 - `metrics`: contains the metrics factory that will set-up the `codahale` library with the metrics that will be collected and where they will be saved.
 - `sink`: contains all the sinks implemented for the Storm version of DSPBench. Any new sink implementation should go here and extend the `BaseSink` class.
 - `spout`: contains all the spouts implemented for the Storm version of DSPBench. Any new spout implementation should go here and extend the `AbstractSpout` class.
 - `tools`: contains a few common helpers that might be used by multiple applications, like the rolling counter logic.
 - `topology`: contains the `AbstractTopology` and `BasicTopology` abstrac classes. All applications that have only a single spout and sink should use `BasicTopology`, others will need to use the `AbstractTopology`.
 - `util`: contains any utility code used by multiple applications or that even if used by a single application contain code that does not have any business logic for a single application.
 - `StormRunner`: this is the main class that will be used as the `main` class to run all applications. It works as a front end that receives the name of the application that will run and its configuration serialized, sets up the application and runs it.

### Application Structure

#### Topology

An application starts with a `Topology` which is the class that will glue together all components that make the Storm application (Spouts and Bolts).
Most topologies extend the `BasicTopology` class which is an abstract class that already sets up one Spout that will be the source of the data stream and one Sink Bolt that will receive the results of the application.
On top of the basic topology there's a `initialize` method where variables, classes and configurations will be set-up and retrieved.
And the `buildTopology` method will be called to actually build the DAG for Storm, gluing together the spout with the bolts that will process and transform the data.
The end result is a `StormTopology` object. Each application has a unique prefix that will be used by the configuration files and it will be defined with the method `getConfigPrefix` of the topology.

#### Spouts

On DSPBench spouts are generic, they are implemented per technology, like a file, socker or Kafka spout. On top of the basic spout provided by Storm we created an abstract spout that extends it and adds some basic funcionaly
like a parser interface that enables the applications to create and define a `Parser` to parse and format the raw data from the spout into a data stream with a defined schema.

There's also the option to use the `GeneratorSpout` and provide a custom data generator that will generate data on demand for the application.

#### Bolts

DSPBench provides an `AbstractBolt` with a few helpers on top of the standard bolt from Storm, but nothing too complex. Each application will need to implement its own bolts with the required business logic to process data and output new data streams.

#### Sink Bolts

A sink is nothing more than a Bolt that does not output any data streams. It only receives the final results from the application and sends it somewhere else, like a database, filesystem, network, console, etc.

### Running in a Cluster

 1. Build the fat JAR (`make build-native`)
 2. Move the JAR file to the Nimbus machine (master)
 3. Move the [bin/](https://github.com/GMAP/DSPBench/blob/v2/dspbench-storm/bin/) folder to the Nimbus machine (master) with all of its contents.
 4. Copy the `.properties` file of the application that you are going to run from the [config](https://github.com/GMAP/DSPBench/tree/v2/dspbench-storm/src/main/resources/config) folder to the Nimbus machine (master). You will need to change the configuration according to what you want, like setting up the spout and sink.
 5. Run the shell script to submit the application: `bin/dspbench-storm-cluster.sh dspbench-storm-uber-1.0.jar wordcount wordcount.properties`.
 6. Open the Storm Web UI to monitor the application (usually at `http://<storm-ui-server>:8080`).
