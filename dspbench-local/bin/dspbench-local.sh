#!/bin/bash

usage() {
    echo "Usage: $(basename $0) <platform> <jar> <task_class> <topology_name> <system_config> <app_config>" && exit 1
}
[ "$#" -ge 4 ] || usage

SCRIPT_PATH=$( cd $(dirname $0) ; pwd -P )
BASE_DIR=$(dirname $0)
JAR_PATH=$1
JAR_FILE=$(basename $JAR_PATH)
JAR_DIR=$(dirname $JAR_PATH)
TASK_CLASS=$2
TOPOLOGY_NAME=$3
SYSTEM_CONFIG=$4
APP_CONFIG=$5

# prepare configuration string
# serialize properties file
SYSTEM_CONFIG_STR=$(java -jar $BASE_DIR/lib/properties-serializer.jar $SYSTEM_CONFIG)
APP_CONFIG_STR=$(java -jar $BASE_DIR/lib/properties-serializer.jar $APP_CONFIG)

# functions
function readProperty() {
    FILE=$1
    PROPERTY=$2
    PROPERTY_VALUE=`sed '/^\#/d' $FILE | grep $PROPERTY  | tail -n 1 | cut -d "=" -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'`
    echo $PROPERTY_VALUE
}

echo "Deploying to $PLATFORM"
echo "$SYSTEM_CONFIG_STR"
echo "java -Xms6G -Xmx6G -cp $JAR_PATH com.streamer.topology.impl.LocalTaskRunner -task $TASK_CLASS -name $TOPOLOGY_NAME -config $SYSTEM_CONFIG_STR"

java -server -XX:+UseG1GC -Xms4g -Xmx6g -XX:+UseCompressedOops -cp $JAR_PATH com.streamer.topology.impl.LocalTaskRunner -task $TASK_CLASS -name $TOPOLOGY_NAME -config $SYSTEM_CONFIG_STR