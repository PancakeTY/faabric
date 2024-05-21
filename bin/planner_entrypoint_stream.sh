#!/bin/bash

set -e

# The planner needs to support being used both from faabric and from faasm,
# and in both cases it must support mounting the built binaries to update it.
# Thus, we add a wrapper around the entrypoint command that takes as an input
# the binary dir, and waits until the binary file exists

BINARY_DIR=${1:-/build/faabric/static/bin}
BINARY_FILE=${BINARY_DIR}/planner_server
CODE_DIR="/code/faabric"
GIT_REPO="https://github.com/PancakeTY/faabric"
BRANCH="state"

# In the developing stage, without stable version, we have to check if the code is
# up to date and rebuild the project if necessary.
# Ensure the code directory exists
if [ ! -d "${CODE_DIR}" ]; then
    mkdir -p ${CODE_DIR}
    git clone -b ${BRANCH} ${GIT_REPO} ${CODE_DIR}
fi

cd ${CODE_DIR}

# Pull the latest changes
echo "Checking for updates in the Git repository..."
git fetch origin ${BRANCH}
LOCAL_COMMIT=$(git rev-parse HEAD)
REMOTE_COMMIT=$(git rev-parse origin/${BRANCH})

if [ "$LOCAL_COMMIT" != "$REMOTE_COMMIT" ]; then
    echo "New changes detected. Updating the repository and rebuilding the project..."
    git pull origin ${BRANCH}
    source venv/bin/activate
    inv dev.cc planner_server
else
    echo "The repository is already up to date."
fi

until test -f ${BINARY_FILE}
do
    echo "Waiting for planner server binary to be available at: ${BINARY_FILE}"
    sleep 3
done

# Once the binary file is available, run it
${BINARY_FILE}
