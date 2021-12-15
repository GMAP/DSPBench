#Dockerfile vars
alpver=3.12
kctlver=1.18.0

#vars
IMAGENAME=my_kubectl
REPO=my.registry
IMAGEFULLNAME=${REPO}/${IMAGENAME}:${KUBECTL_VERSION}

.PHONY: help build push all

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

build:
	    @docker build --pull --build-arg ALP_VER=${alpver} --build-arg KCTL_VER=${kctlver} -t ${IMAGEFULLNAME} .

push:
	    @docker push ${IMAGEFULLNAME}

all: build push