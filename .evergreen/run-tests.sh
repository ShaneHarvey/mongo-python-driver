#!/bin/sh
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Defaults to "noauth"
#       SSL                     Set to enable SSL. Defaults to "nossl"
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       MARCH                   Machine Architecture. Defaults to lowercase uname -m


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
[ -z "$MARCH" ] && MARCH=$(uname -m | tr '[:upper:]' '[:lower:]')


if [ "$AUTH" != "noauth" ]; then
  export DB_USER="bob"
  export DB_PASSWORD="pwd123"
fi

if [ "$SSL" != "nossl" ]; then
   export CLIENT_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/client.pem"
   export CA_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/ca.pem"
fi

PYTHON=$(command -v python || command -v python3)

if [ "$PYTHON" == "" ]; then
    echo "Cannot test without python or python3 installed!"
    exit 1
fi

# virtualenv needs a Windows style path (C:/...) to the Python interpreter
case "$OS" in
   cygwin*)
      PYTHON=$(cygpath -m "$PYTHON")
      ;;
esac

echo "Installing nose..."
virtualenv -p "$PYTHON" venv
if [ -f venv/bin/activate ]; then
  . venv/bin/activate
elif [ -f venv/Scripts/activate ]; then
  . venv/Scripts/activate
fi
pip install nose

echo "Running $AUTH tests over $SSL with python $PYTHON, connecting to $MONGODB_URI"
python -c 'import sys; print(sys.version)'

# Run the tests, and store the results in the Evergreen compatible XUnit XML
# file nosetests.xml.

python setup.py clean
python setup.py nosetests
