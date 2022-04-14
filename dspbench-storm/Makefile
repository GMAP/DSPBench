#Dockerfile vars


#vars
IMAGENAME=dspbench-storm
APP?=wordcount
PACKAGE="org.dspbench.applications"

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
	./gradlew clean build shadowJar

build-docker:
	@docker build -t ${IMAGENAME} .

run-docker:
	@echo "Running $(APP) on Docker..."

	@if [ $(APP) = "adsanalytics" ]; then\
		docker run -it ${IMAGENAME} adsanalytics /app/config/ads-analytics.properties;\
	fi

	@if [ $(APP) = "clickanalytics" ]; then\
		docker run -it ${IMAGENAME} clickanalytics /app/config/click-analytics.properties;\
	fi

	@if [ $(APP) = "frauddetection" ]; then\
		docker run -it ${IMAGENAME} frauddetection /app/config/fraud-detection.properties;\
	fi

	@if [ $(APP) = "logprocessing" ]; then\
		docker run -it ${IMAGENAME} logprocessing /app/config/log-processing.properties;\
	fi

	@if [ $(APP) = "machineoutlier" ]; then\
		docker run -it ${IMAGENAME} machineoutlier /app/config/machine-outlier.properties;\
	fi

	@if [ $(APP) = "reinforcementlearner" ]; then\
		docker run -it ${IMAGENAME} reinforcementlearner /app/config/reinforcement-learner.properties;\
	fi

	@if [ $(APP) = "sentimentanalysis" ]; then\
		 docker run -it ${IMAGENAME} sentimentanalysis /app/config/sentiment-analysis.properties;\
	fi

	@if [ $(APP) = "spamfilter" ]; then\
		docker run -it ${IMAGENAME} spamfilter /app/config/spam-filter.properties;\
	fi

	@if [ $(APP) = "spikedetection" ]; then\
		docker run -it ${IMAGENAME} spikedetection /app/config/spike-detection.properties;\
	fi

	@if [ $(APP) = "trafficmonitoring" ]; then\
		docker run -it ${IMAGENAME} trafficmonitoring /app/config/traffic-monitoring.properties;\
	fi

	@if [ $(APP) = "trendingtopics" ]; then\
		docker run -it ${IMAGENAME} trendingtopics /app/config/trending-topics.properties;\
	fi

	@if [ $(APP) = "wordcount" ]; then\
		docker run -it ${IMAGENAME} wordcount /app/config/word-count.properties;\
	fi

	@if [ $(APP) = "smartgrid" ]; then\
		docker run -it ${IMAGENAME} smartgrid /app/config/smart-grid.properties;\
	fi

	@if [ $(APP) = "voipstream" ]; then\
		docker run -it ${IMAGENAME} voipstream /app/config/voip-stream.properties;\
	fi

	@if [ $(APP) = "bargainindex" ]; then\
		docker run -it ${IMAGENAME} bargainindex /app/config/bargain-index.properties;\
	fi

	@if [ $(APP) = "smartgrid" ]; then\
		docker run -it ${IMAGENAME} smartgrid /app/config/smart-grid.properties;\
	fi

all: build