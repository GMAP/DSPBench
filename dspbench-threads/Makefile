#Dockerfile vars


#vars
IMAGENAME=dspbench-threads
APP?=wordcount
PACKAGE="com.streamer.examples"

.PHONY: help build-docker all

help:
	@echo "Makefile arguments:"
	@echo ""
	@echo "alpver - Alpine Version"
	@echo "kctlver - kubectl version"
	@echo ""
	@echo "Makefile commands:"
	@echo "build"
	@echo "push"
	@echo "all"

.DEFAULT_GOAL := all

build-native:
	./gradlew clean build

build-docker:
	@docker build -t ${IMAGENAME} .

run-docker:
	@echo "Running $(APP) on Docker..."

	@if [ $(APP) = "adsanalytics" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.adsanalytics.AdsAnalyticsTask AdsAnalytics /app/config/ads-analytics.properties;\
	fi

	@if [ $(APP) = "clickanalytics" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.clickanalytics.ClickAnalyticsTask ClickAnalytics /app/config/click-analytics.properties;\
	fi

	@if [ $(APP) = "frauddetection" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.frauddetection.FraudDetectionTask FraudDetection /app/config/fraud-detection.properties;\
	fi

	@if [ $(APP) = "logprocessing" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.logprocessing.LogProcessingTask LogProcessing /app/config/log-processing.properties;\
	fi

	@if [ $(APP) = "machineoutlier" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.machineoutlier.MachineOutlierTask MachineOutlier /app/config/machine-outlier.properties;\
	fi

	@if [ $(APP) = "reinforcementlearner" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.reinforcementlearner.ReinforcementLearnerTask ReinforcementLearner /app/config/reinforcement-learner.properties;\
	fi

	@if [ $(APP) = "sentimentanalysis" ]; then\
		 docker run -it ${IMAGENAME} ${PACKAGE}.sentimentanalysis.SentimentAnalysisTask SentimentAnalysis /app/config/sentiment-analysis.properties;\
	fi

	@if [ $(APP) = "spamfilter" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.spamfilter.SpamFilterTask SpamFilter /app/config/spam-filter.properties;\
	fi

	@if [ $(APP) = "spikedetection" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.spikedetection.SpikeDetectionTask SpikeDetection /app/config/spike-detection.properties;\
	fi

	@if [ $(APP) = "trafficmonitoring" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.trafficmonitoring.TrafficMonitoringTask TrafficMonitoring /app/config/traffic-monitoring.properties;\
	fi

	@if [ $(APP) = "trendingtopics" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.trendingtopics.TrendingTopicsTask TrendingTopics /app/config/trending-topics.properties;\
	fi

	@if [ $(APP) = "wordcount" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.wordcount.WordCountTask WordCount /app/config/word-count.properties;\
	fi

	@if [ $(APP) = "smartgrid" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.smartgrid.SmartGridTask SmartGrid /app/config/smart-grid.properties;\
	fi

	@if [ $(APP) = "voipstream" ]; then\
		docker run -it ${IMAGENAME} ${PACKAGE}.voipstream.VoIPSTREAMTask VoIPSTREAM /app/config/voip-stream.properties;\
	fi

all: build