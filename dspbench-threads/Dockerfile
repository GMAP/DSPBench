# # using multistage docker build
# # ref: https://docs.docker.com/develop/develop-images/multistage-build/
#
# # temp container to build using gradle
#FROM gradle:5.3.0-jdk-alpine AS TEMP_BUILD_IMAGE
# ENV APP_HOME=/usr/app/
# WORKDIR $APP_HOME
# COPY build.gradle $APP_HOME
#
# # COPY ../gradle $APP_HOME/gradle
# COPY --chown=gradle:gradle . /home/gradle/src
# USER root
# RUN chown -R gradle /home/gradle/src
#
# RUN gradle build || return 0
# COPY . .
# RUN gradle clean build shadowJar
#
# # actual container
# FROM adoptopenjdk/openjdk11:alpine-jre
# ENV ARTIFACT_NAME=dspbench-local-1.0-all.jar
# ENV APP_HOME=/usr/app/
#
# WORKDIR $APP_HOME
# COPY --from=TEMP_BUILD_IMAGE $APP_HOME/build/libs/$ARTIFACT_NAME .
#
# EXPOSE 8080
# ENTRYPOINT exec java -jar ${ARTIFACT_NAME}

FROM gradle:6.5.1-jdk11 AS build

COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle clean build shadowJar --no-daemon
RUN ls /home/gradle/src/build/libs/

FROM openjdk:11-jre-slim

RUN mkdir /app
RUN mkdir /app/lib

COPY --from=build /home/gradle/src/build/libs/src-uber-1.0.jar /app/application.jar
COPY bin/lib/properties-serializer.jar /app/lib/properties-serializer.jar
COPY bin/dspbench-local.sh /app/dspbench-local.sh
COPY data /app/data
COPY src/main/resources/config/ /app/config

ENTRYPOINT ["/app/dspbench-local.sh", "/app/application.jar"]