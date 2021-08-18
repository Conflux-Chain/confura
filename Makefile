# The output binary name
BINARY=bin/conflux-infura

# Values to pass for VERSION and BUILD etc.,
# eg., git tag 1.0.1 then git commit -am "One more change after the tags"
ifndef VERSION
	VERSION=`git describe --tags` # Shall we need --exact-match?
endif

BUILD_DATE=`date +%FT%T%z`
GIT_COMMIT=`git rev-parse HEAD`

# Setup the -ldflags option for go build here, interpolate the variable values
PKG=github.com/conflux-chain/conflux-infura/config
LDFLAGS=-ldflags "-w -s -X ${PKG}.Version=${VERSION} -X ${PKG}.BuildDate=${BUILD_DATE} -X ${PKG}.GitCommit=${GIT_COMMIT}"

# Build the project
build:
	go build ${LDFLAGS} -o ${BINARY}

# Install project: copy binaries
install:
	go install ${LDFLAGS}

# Clean project: delete binaries
clean:
	@if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi

.PHONY: build clean install