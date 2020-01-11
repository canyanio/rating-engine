#!/bin/sh

set -e

if ! [ -z "$RATING_ENGINE_MESSAGEBUS_URI" ]; then
    HOST_AND_PORT=$(echo $RATING_ENGINE_MESSAGEBUS_URI | python -c "import sys; from urlparse import urlparse; data = sys.stdin.read().strip(); result = urlparse(data); host = result.netloc.split('@', 1)[-1]; sys.stdout.write(host)")
    echo "Waiting for ${HOST_AND_PORT}..."
    /usr/bin/wait-for $HOST_AND_PORT -t 60
fi

exec $@
