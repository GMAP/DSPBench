# dspbench-spark

This is the implementation of the DSPBench stream processing benchmark for Apache Spark Streaming.

## Project Information

This project is developed with Java 11 and uses Gradle as the build tool. All build configuration and dependencies are on `build.gralde`.
The project can be built directly with `gradlew` or using Docker for testing purposes. We have also a `Makefile` to help running some basic commands:

 - `make build-native`: to build the project and generate a fat JAR with all project dependencies (ready for deployment)
 - `make build-docker`: to build a Docker image with Java and the fat JAR to run Spark as a standalone application.
 - `make run-docker APP=<app-name>`: to run the built Docker image with the application defined as argument.

### Code Structure

 - `application`: this package contains subfolders, one per application, and within each application folder we have all the code of the application (except for maybe a few utilities that are more generic).
 - `functions`: contains the abstract bolt class that all bolts must extend.
 - `constants`: contains the base constants interface with configuration keys that can be used for all applications. All configuration keys start with a mask because this mask will be replace by the application prefix. For example the key `%s.spout.threads` will be `wc.spout.threads` for the `wordcount` application.
 - `hooks`: hooks are a functionality provided by Spark to run small pieces of code before and after a tuple is executed within a spout or bolt. This can be used to gather metrics about the application.
 - `metrics`: contains the metrics factory that will set-up the `codahale` library with the metrics that will be collected and where they will be saved.
 - `sink`: contains all the sinks implemented for the Spark version of DSPBench. Any new sink implementation should go here and extend the `BaseSink` class.
 - `source`: contains all the spouts implemented for the Spark version of DSPBench. Any new spout implementation should go here and extend the `AbstractSpout` class.
 - `util`: contains any utility code used by multiple applications or that even if used by a single application contain code that does not have any business logic for a single application.
 - `SparkStreamingRunner`: this is the main class that will be used as the `main` class to run all applications. It works as a front end that receives the name of the application that will run and its configuration serialized, sets up the application and runs it.

### Application Structure

#### App

An application starts with a `AbstractApplication` which is the class that will glue together all components that make the Spark application (Sources and Functions).

#### Sources

On DSPBench sources are generic, they are implemented per technology, like a file, socker or Kafka source.

#### Functions

DSPBench provides an `BaseFunction` with a few helpers on top of the standard functions from Spark, but nothing too complex. Each application will need to implement its own functions with the required business logic to process data and output new data streams.

#### Sink Functions

A sink is nothing more than a function that does not output any data streams. It only receives the final results from the application and sends it somewhere else, like a database, filesystem, network, console, etc.

### Running in a Cluster

 1. Build the fat JAR (`make build-native`)
 2. Move the JAR file to the master machine
 3. Move the [bin/](https://github.com/GMAP/DSPBench/blob/v2/dspbench-spark/bin/) folder to the master machine with all of its contents.
 4. Copy the `.properties` file of the application that you are going to run from the [config](https://github.com/GMAP/DSPBench/tree/v2/dspbench-spark/src/main/resources/config) folder to the master machine. You will need to change the configuration according to what you want, like setting up the source and sink.
 5. Run the shell script to submit the application: `bin/dspbench-spark-cluster.sh dspbench-spark-uber-1.0.jar wordcount wordcount.properties`.
 6. Open the Spark UI to monitor the application (usually at `http://<master-server>:4040`).
