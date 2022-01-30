# dspbench-threads

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
bin/dspbench-threads.sh build/libs/dspbench-threads-1.0-all.jar org.dspbench.applications.wordcount.WordCountTask WordCount src/main/resources/config/word-count.properties 
```

## Run on Docker

Build the image:
```
docker build -t dspbench-threads .
```

Run an application:
```
docker run -it dspbench-threads org.dspbench.applications.wordcount.WordCountTask WordCount /app/config/word-count.properties
```
