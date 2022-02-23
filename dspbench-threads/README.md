# dspbench-threads

This is the implementation of the DSPBench stream processing benchmark for running locally with multithreading.

## Requirements

 - JDK 11

## Project Information

This project is developed with Java 11 and uses Gradle as the build tool. All build configuration and dependencies are on `build.gralde`.
The project can be built directly with `gradlew` or using Docker for testing purposes. We have also a `Makefile` to help running some basic commands:

 - `make build-native`: to build the project and generate a fat JAR with all project dependencies (ready for deployment)
 - `make build-docker`: to build a Docker image with Java and the fat JAR to run the application.
 - `make run-docker APP=<app-name>`: to run the built Docker image with the application defined as argument.

### Code Structure

 - `applications`: this package contains subfolders, one per application, and within each application folder we have all the code of the application (except for maybe a few utilities that are more generic).
 - `base/operators`: contains the abstract operator class that all operators must extend.
 - `base/constants`: contains the base constants interface with configuration keys that can be used for all applications. All configuration keys start with a mask because this mask will be replace by the application prefix. For example the key `%s.source.threads` will be `wc.spout.threads` for the `wordcount` application.
 - `core/hook`: hooks are a functionality to run small pieces of code before and after a tuple is executed within a source or operator. This can be used to gather metrics about the application.
 - `metrics`: contains the metrics factory that will set-up the `codahale` library with the metrics that will be collected and where they will be saved.
 - `base/sink`: contains all the sinks implemented for the Storm version of DSPBench. Any new sink implementation should go here and extend the `BaseSink` class.
 - `base/source`: contains all the sources implemented for the threads version of DSPBench. Any new source implementation should go here and extend the `AbstractSource` class.
 - `base/task`: contains the `AbstractTask` and `BasicTask` abstrac classes. All applications that have only a single source and sink should use `BasicTask`, others will need to use the `AbstractTask`.
 - `util`: contains any utility code used by multiple applications or that even if used by a single application contain code that does not have any business logic for a single application.
 - `core`: contains all basic classes necessary for building a threads data stream application.
 - `topology`: contains the interfaces and implementations of the components of the threads engine, like adapters that will run the operators and sources as well as the thread pools that will run them.

### Application Structure

#### Task

An application starts with a `Task` which is the class that will glue together all components that make the threads application (Sources and Operators).
Most tasks extend the `BasicTask` class which is an abstract class that already sets up one Source that will be the source of the data stream and one Sink Operator that will receive the results of the application.
On top of the basic task there's a `setConfiguration` method where variables, classes and configurations will be set-up and retrieved.
And the `initialize` method will be called to actually build the DAG for threads, gluing together the source with the operators that will process and transform the data.
The end result is a plan object with all the components and its relationshipts. Each application has a unique prefix that will be used by the configuration files and it will be defined with the method `getConfigPrefix` of the task.

#### Source

On DSPBench sources are generic, they are implemented per technology, like a file, socker or Kafka source. We created an abstract source that adds some basic funcionaly for the source and helper functions, like a parser interface that enables the applications to create and define a `Parser` to parse and format the raw data from the source into a data stream with a defined schema.

There's also the option to use the `GeneratorSource` and provide a custom data generator that will generate data on demand for the application.

#### Operators

DSPBench provides an `AbstractOperator` with a few helpers on top of the standard operator, but nothing too complex. Each application will need to implement its own operators with the required business logic to process data and output new data streams.

#### Sink Operators

A sink is nothing more than an Operator that does not output any data streams. It only receives the final results from the application and sends it somewhere else, like a database, filesystem, network, console, etc.

## Usage

### Run Native

To build the jar:
```
../gradlew clean build shadowJar -x test
```

To run an application:

```
bin/dspbench-threads.sh build/libs/dspbench-threads-1.0-all.jar org.dspbench.applications.wordcount.WordCountTask WordCount src/main/resources/config/word-count.properties 
```

### Run on Docker

Build the image:
```
docker build -t dspbench-threads .
```

Run an application:
```
docker run -it dspbench-threads org.dspbench.applications.wordcount.WordCountTask WordCount /app/config/word-count.properties
```
