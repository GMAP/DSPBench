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
COPY bin/dspbench-threads.sh /app/dspbench-threads.sh
COPY data /app/data
COPY src/main/resources/config/ /app/config

ENTRYPOINT ["/app/dspbench-threads.sh", "/app/application.jar"]
