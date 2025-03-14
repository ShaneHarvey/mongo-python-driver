#!/bin/bash

set -o xtrace
set -o errexit

git clone --depth 1 https://github.com/mongodb/specifications.git
pushd specifications/source/benchmarking/data
tar xf extended_bson.tgz
tar xf parallel.tgz
tar xf single_and_multi_document.tgz
popd

export TEST_PATH="${PROJECT_DIRECTORY}/specifications/source/benchmarking/data"
export OUTPUT_FILE="${PROJECT_DIRECTORY}/results.json"

export PYTHON_BINARY=/opt/mongodbtoolchain/v4/bin/python3

bash ./.evergreen/just.sh setup-tests perf "${SUB_TEST_NAME}"
bash ./.evergreen/just.sh run-tests
