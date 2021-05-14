#!/bin/bash

# Determine if use pm2 or not
USE_PM2=false
if  [[ $1 = "--pm2" ]]; then
    echo "Use pm2 for daemon guard..."
    USE_PM2=true
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
    if pgrep -x "$PROJECT" 2>&1 > /dev/null
    then
        pgrep -x "$PROJECT" | xargs kill
        check "Kill $PROJECT"
    else
        echo "No $PROJECT is running, skip killing"
    fi
}

pm2_kill() {
    if pm2 describe $PROJECT 2>&1 > /dev/null
    then
        pm2 delete $PROJECT
        check "Kill $PROJECT"
    else
        echo "No $PROJECT is running, skip killing"
    fi
}

# Kill the old process if necessary
if [ "$USE_PM2" = true ]; then
    pm2_kill
else
    pgrep_kill
fi

# Run new process
nohup_run() {
    mkdir -p logs
    NOW=`date +'%Y_%m_%d_%H_%M_%S'`
    LOGFILE="./logs/nohup_conflux-infura_$NOW.log"
    nohup bin/$PROJECT > $LOGFILE 2>&1 &
    check "Run $PROJECT"
}

pm2_run() {
    mkdir -p logs
    NOW=`date +'%Y_%m_%d_%H_%M_%S'`
    LOGFILE="./logs/pm2_conflux-infura_$NOW.log"
    pm2 start --name $PROJECT --log $LOGFILE --time bin/$PROJECT
    check "Run $PROJECT"
}

# Start up a new background process
if [ "$USE_PM2" = true ]; then
    pm2_run
else
    nohup_run
fi

# Quit
quit