#!/bin/sh

services='stomp-bridge event-writer event-cache event-reader push-api persistence-rethinkdb http-api'

for service in $services; do
    docker run -e EVENTSTORE_RETHINKDB_ADDRESS=172.17.0.2 -i eventstore/$service &
    sleep 10
done
