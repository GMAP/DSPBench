#!/bin/bash

usage() {
    echo "Usage: $(basename $0) <jar> <application_name> <app_config_file>" && exit 1
}
[ "$#" -ge 3 ] || usage

SCRIPT_PATH=$( cd $(dirname $0) ; pwd -P )
BASE_DIR=$(dirname $0)
JAR_PATH=$1
JAR_FILE=$(basename $JAR_PATH)
JAR_DIR=$(dirname $JAR_PATH)
APP_NAME=$2
APP_CONFIG=$3

# prepare configuration string
# serialize properties file
APP_CONFIG_STR=$(java -jar $BASE_DIR/lib/properties-serializer.jar $APP_CONFIG)

# functions
function readProperty() {
    FILE=$1
    PROPERTY=$2
    PROPERTY_VALUE=`sed '/^\#/d' $FILE | grep $PROPERTY  | tail -n 1 | cut -d "=" -f2- | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'`
    echo $PROPERTY_VALUE
}

echo "Deploying to $PLATFORM"
echo "$APP_CONFIG_STR"

storm jar $JAR_PATH org.dspbench.StormRunner --app $APP_NAME --config-str $APP_CONFIG_STR --mode remote
