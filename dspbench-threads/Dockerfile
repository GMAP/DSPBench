FROM gradle:6.5.1-jdk11 AS build

COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle clean build shadowJar --no-daemon

FROM openjdk:11-jre-slim

RUN apt-get update && apt-get install -y unzip

RUN mkdir /app
RUN mkdir /app/lib

COPY --from=build /home/gradle/src/build/libs/dspbench-threads-uber-1.0.jar /app/application.jar
COPY bin/lib/properties-serializer.jar /app/lib/properties-serializer.jar
COPY bin/dspbench-threads.sh /app/dspbench-threads.sh
COPY data /app/data
COPY src/main/resources/spamfilter /app/spamfilter
COPY src/main/resources/config/ /app/config

RUN unzip /app/spamfilter/wordmap.zip -d /app/spamfilter/

ENTRYPOINT ["/app/dspbench-threads.sh", "/app/application.jar"]
