#!/bin/bash -e

# give some time to timetrails db
sleep 10
./docker-scripts/wait-for-it.sh monetdb:50000 -s -- echo "MonetDB is up"

EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "Starting guardian ..."
    #/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
    exec python -u app.py -dh monetdb
fi
