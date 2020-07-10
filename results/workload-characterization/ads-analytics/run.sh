#!/bin/bash

cd /media/mayconbordin/data/projects/data-stream-benchmark/streamer/bin

./streamer local /home/mayconbordin/tmp/streamer-local-FINAL2.jar:/home/mayconbordin/tmp/streamer-examples-FINAL2.jar com.streamer.examples.adsanalytics.AdsAnalyticsTask AdsAnalytics /media/mayconbordin/data/projects/data-stream-benchmark/new-wc/ads-analytics/config/config.properties
