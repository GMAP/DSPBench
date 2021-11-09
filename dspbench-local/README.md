# dspbench-local

Local and multithreaded version of the benchmark.

## Requirements

 - JDK 11

## Run Native

To build the jar:
```
../gradlew clean build shadowJar -x test
```

To run an application:

```
bin/dspbench-local.sh build/libs/dspbench-local-1.0-all.jar com.streamer.examples.wordcount.WordCountTask WordCount src/main/resources/config/word-count.properties 
```

## Run on Docker

Build the image:
```
docker build -t dspbench-local .
```

Run an application:
```
docker run -it dspbench-local com.streamer.examples.wordcount.WordCountTask WordCount /app/config/word-count.properties
```
