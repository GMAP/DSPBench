#!/bin/bash

cd /home/mayconbordin/Dropbox/programming/streamer/bin

./streamer local ../streamer-examples/target/streamer-examples-1.1-jar-with-dependencies.jar com.streamer.examples.wordcount.WordCountTask WordCount ../streamer-examples/src/main/resources/wordcount/config.properties
