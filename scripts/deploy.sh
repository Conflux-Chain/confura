#!/bin/bash

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

# Build the project
cd $BUILD_DIR
mkdir -p bin
rm -fr bin/$PROJECT
go build -o bin/$PROJECT main.go

check "Build $PROJECT"

# Kill the old process if necessary
if pgrep -x "$PROJECT" 2>&1 > /dev/null
then
    pgrep -x "$PROJECT" | xargs kill -9
    check "Kill $PROJECT"
else
    echo "No $PROJECT is running, skip killing"
fi

# Start up a new background process
mkdir -p logs
now=`date +'%Y_%m_%d_%H_%M_%S'`
nohup bin/$PROJECT > ./logs/conflux-infura_$now.log 2>&1 &

check "Run $PROJECT"

quit



