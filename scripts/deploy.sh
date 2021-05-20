#!/bin/bash

# Config switches with default value
USE_PM2=false       # if use pm2 or not
INSTANCE_NAME=""    # runtime instance name
RUNTIME_ARGS=""     # runtime argument(s) passed to binary executable

usage()
{
  echo "Usage: deploy.sh [ -p | --pm2 ] [ -n | --name InstanceName ] -- RuntimeArgument(s)"
  exit 2
}

# Get command line options, refer to https://www.shellscript.sh/tips/getopt/
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -n|--name) INSTANCE_NAME="$2"; shift 2 ;;
        -p|--pm2)  USE_PM2=true; shift ;;
        --) RUNTIME_ARGS="$2"; shift 2 ;;
        *) usage ;;
    esac
done

# Dump config switches if set
if $USE_PM2; then
    echo "Using pm2 for daemon guard ..."
fi

if [ ! -z "$INSTANCE_NAME" ]; then
    echo "Using instance name ${INSTANCE_NAME} ..."
fi

if [ ! -z "$RUNTIME_ARGS" ]; then
    echo "Using runtime argument(s) ${RUNTIME_ARGS} ..."
fi

# Save the pwd before we run anything
PRE_PWD=`pwd`

# Determine the build script's actual directory, following symlinks
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
BUILD_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
BUILD_DIR="$(dirname "$BUILD_DIR")"

# Derive the project name from the directory
PROJECT="$(basename $BUILD_DIR)"

# Runtime instance name
RUNTIME_INSTANCE="${PROJECT}"
if [ ! -z "${INSTANCE_NAME}" ]; then
    RUNTIME_INSTANCE="${RUNTIME_INSTANCE}-${INSTANCE_NAME}"
fi

# Setup the environment for the build
GOPATH=$BUILD_DIR:$GOPATH

# Set exit status
EXIT_STATUS=0

# Define quit command
quit() {
    # Change back to where we were
    cd $PRE_PWD

    exit $EXIT_STATUS
}

# Check execution status
check() {
    EXIT_STATUS=$?
    TASK=$1
    
    if [ $EXIT_STATUS == 0 ]; then
        echo "$TASK succeeded"
    else
        echo "$TASK failed !!!"
        quit
    fi
}

# Build binary executable
build() {
    mkdir -p bin
    rm -fr bin/$PROJECT
    go build -o bin/$PROJECT main.go
    check "Build $PROJECT"
}

# Build the project
cd $BUILD_DIR
build

# Kill old process
pgrep_kill() {
    if pgrep -x "$RUNTIME_INSTANCE" 2>&1 > /dev/null
    then
        pgrep -x "$RUNTIME_INSTANCE" | xargs kill
        check "Kill $RUNTIME_INSTANCE"
    else
        echo "$RUNTIME_INSTANCE is not running, skip killing"
    fi
}

pm2_kill() {
    if pm2 describe $RUNTIME_INSTANCE 2>&1 > /dev/null
    then
        pm2 delete $RUNTIME_INSTANCE
        check "Kill $RUNTIME_INSTANCE"
    else
        echo "$RUNTIME_INSTANCE is not running, skip killing"
    fi
}

# Kill the old process if necessary
if [ "$USE_PM2" = true ]; then
    pm2_kill
    sleep 5 # sleep 5s for process to exit
else
    pgrep_kill
    sleep 5 # sleep 5s for process to exit
fi

# Run new process
nohup_run() {
    mkdir -p logs
    NOW=`date +'%Y_%m_%d_%H_%M_%S'`
    LOGFILE="./logs/nohup_${RUNTIME_INSTANCE}_$NOW.log"
    nohup bin/$PROJECT ${RUNTIME_ARGS} > $LOGFILE 2>&1 &
    check "Run ${RUNTIME_INSTANCE}"
}

pm2_run() {
    mkdir -p logs
    NOW=`date +'%Y_%m_%d_%H_%M_%S'`
    LOGFILE="./logs/pm2_${RUNTIME_INSTANCE}_$NOW.log"
    pm2 start --name ${RUNTIME_INSTANCE} --log $LOGFILE --time bin/$PROJECT ${RUNTIME_ARGS}
    check "Run ${RUNTIME_INSTANCE}"
}

# Start up a new background process
if [ "$USE_PM2" = true ]; then
    pm2_run
else
    nohup_run
fi

# Quit
quit