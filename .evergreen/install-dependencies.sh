#!/bin/sh
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Copy PyMongo's test certificates over driver-evergreen-tools'
cp ${PROJECT_DIRECTORY}/test/certificates/* ${DRIVERS_TOOLS}/.evergreen/x509gen/

# Replace MongoOrchestration's client certificate.
cp ${PROJECT_DIRECTORY}/test/certificates/client.pem ${MONGO_ORCHESTRATION_HOME}/lib/client.pem
