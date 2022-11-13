#vars

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

all: build
