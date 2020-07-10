#!/bin/bash

cd /home/mayconbordin/Dropbox/programming/streamer/bin

./streamer local ../streamer-examples/target/streamer-examples-1.1-jar-with-dependencies.jar com.streamer.examples.clickanalytics.ClickAnalyticsTask ClickAnalytics ../streamer-examples/src/main/resources/clickanalytics/config.properties
