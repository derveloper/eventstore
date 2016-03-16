#!/bin/sh

services='stomp-bridge event-writer event-cache event-reader push-api persistence-rethinkdb http-api'

for service in $services; do
    docker push vileda/es-$service
done
